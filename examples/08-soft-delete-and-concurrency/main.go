// 第 8 章：软删除和并发。删除与恢复、乐观锁冲突与重试、行锁与方言能力。
//
// 运行：go run ./examples/08-soft-delete-and-concurrency
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/dialect"
	"github.com/tmoeish/tsq/v5/examples/internal/show"
	"github.com/tmoeish/tsq/v5/examples/shop"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var product = shop.TableProduct

// liveCount 数"看得见"的商品：products 声明了 deleted_at，任何查询都只看没删除的行。
var liveCount = tsq.Select(product.Columns()...).From(product).MustBuild()

// allCount 通过 WithDeleted() 连已删除的也数上——审计时才需要。
var allCount = tsq.Select(product.Columns()...).From(product.WithDeleted()).MustBuild()

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "8.1 Delete 是软删除：行还在，只是从所有查询里消失")

	lamp, err := product.GetBySKU(ctx, db, "P-4002")
	if err != nil {
		return err
	}

	// 软删除写 deleted_at、updated_at 和 version，其他改动不保存。
	if err := lamp.Delete(ctx, db); err != nil {
		return err
	}

	live, err := liveCount.Count(ctx, db)
	if err != nil {
		return err
	}

	all, err := allCount.Count(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "已删除：%t；看得见的商品 %d 件，含已删除的 %d 件", lamp.IsDeleted(), live, all)

	// 按 SKU 找不到了——GetBySKU 同样只看没删除的行。
	gone, err := product.FindBySKU(ctx, db, "P-4002")
	if err != nil {
		return err
	}

	show.Resultf(w, "FindBySKU(P-4002)：%v", gone)

	// ---------------------------------------------------------------------
	show.Step(w, "8.2 对已删除的行再 Delete：RowStateError，重试没有用")

	err = lamp.Delete(ctx, db)
	if stateErr, ok := errors.AsType[*tsq.RowStateError](err); ok {
		show.Resultf(w, "Op=%s，Need=%s", stateErr.Op, stateErr.Need)
		show.Resultf(w, "%v", err)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "8.3 Restore：从 WithDeleted() 里把它找回来")
	// 按主键取。已删除的行不再占用唯一值（唯一索引以 deleted_at 打头，同一个 SKU
	// 可以有多个已删除的行），所以在 WithDeleted() 上按 SKU 查找会被拒绝。
	lamp, err = product.WithDeleted().Get(ctx, db, lamp.ID)
	if err != nil {
		return err
	}

	if err := lamp.Restore(ctx, db); err != nil {
		return err
	}

	show.Resultf(w, "恢复后已删除：%t", lamp.IsDeleted())

	// ---------------------------------------------------------------------
	show.Step(w, "8.4 按条件软删除 DeleteFrom，按主键批量软删除 BatchDeleteByPK")
	// DeleteFrom 只接受声明了 deleted_at 的表，渲染成一条打删除标记的 UPDATE。
	// 真删用 HardDeleteFrom / HardDelete——grep Hard 就能找到项目里所有真删数据的地方。
	n, err := tsq.DeleteFrom(product).Where(product.Stock.EQ(tsq.Val(int64(0)))).Exec(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "下架缺货商品：%d 件", n)

	if err := product.BatchDeleteByPK(ctx, db, []int64{7}); err != nil {
		return err
	}

	live, err = liveCount.Count(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "还看得见 %d 件", live)

	// ---------------------------------------------------------------------
	show.Step(w, "8.5 乐观锁：两个人同时改同一件商品，后保存的那个会失败")
	// 两个请求各自读到了 version 相同的同一行。
	alice, err := product.GetBySKU(ctx, db, "P-1001")
	if err != nil {
		return err
	}

	bob, err := product.GetBySKU(ctx, db, "P-1001")
	if err != nil {
		return err
	}

	alice.PriceCents = 379900
	if err := alice.Update(ctx, db); err != nil {
		return err
	}

	// Bob 手里的是旧版本：UPDATE ... WHERE version = 旧版本 匹配不到行。
	// 这不是可以忽略的错误——忽略它就是悄悄覆盖了 Alice 的改动。
	bob.Stock = 45
	err = bob.Update(ctx, db)
	show.Resultf(w, "Bob 保存：%v", err)
	show.Resultf(w, "是乐观锁冲突：%t", tsq.IsOptimisticLockError(err))

	// ---------------------------------------------------------------------
	show.Step(w, "8.6 冲突后重试：在事务里重新读、再改，WithRetry 自动重跑")
	// 回调要自己读它要改的行：重试会重跑整个回调，用的是新读到的版本。
	attempts := 0

	err = db.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
		attempts++

		p, err := product.GetBySKU(ctx, tx, "P-1001")
		if err != nil {
			return err
		}

		if attempts == 1 {
			// 在单进程的演示里制造一次冲突：读完之后用 UpdateTable 改一下这一行，
			// 它会让 version 加一（按条件更新从不校验版本，但总会自增它），
			// 于是下面的 p.Update 按旧版本匹配不到行，返回乐观锁冲突。
			// 事务随之回滚（这次改动也一起撤销），WithRetry 重跑回调，第二次读到的是最新版本。
			if _, err := tsq.UpdateTable(product).
				Set(product.Stock, tsq.Val(int64(44))).
				Where(product.ID.EQ(tsq.Val(p.ID))).
				Exec(ctx, tx); err != nil {
				return err
			}
		}

		p.Stock--

		return p.Update(ctx, tx)
	}, tsq.WithRetry(tsq.IsOptimisticLockError))
	if err != nil {
		return err
	}

	final, err := product.GetBySKU(ctx, db, "P-1001")
	if err != nil {
		return err
	}

	show.Resultf(w, "第 %d 次尝试成功，库存 %d，version %d", attempts, final.Stock, final.Version)

	// ---------------------------------------------------------------------
	show.Step(w, "8.7 行锁 FOR UPDATE：构建时不检查方言，执行时才检查")
	// 同一个 *Query 可以在多种方言上复用，所以方言能力在执行时校验。
	// SQLite 没有行锁，执行时返回 *dialect.UnsupportedCapabilityError，带能力名和方言名。
	locked := tsq.
		Select(product.Columns()...).
		From(product).
		Where(product.ID.EQ(tsq.Val(int64(1)))).
		ForUpdate().
		MustBuild()

	err = db.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
		_, err := locked.Get(ctx, tx)

		return err
	})
	if capErr, ok := errors.AsType[*dialect.UnsupportedCapabilityError](err); ok {
		show.Resultf(w, "%s 不支持 %s", capErr.Dialect, capErr.Capability)
	}

	// 要按方言选择写法，事先问 dialect.Supports；在事务回调里用 tsq.DialectOf(tx) 拿方言。
	for _, name := range []dialect.Name{dialect.SQLite, dialect.MySQL, dialect.Postgres} {
		show.Resultf(w, "%-8s 支持 FOR UPDATE：%t，SKIP LOCKED：%t", name,
			dialect.Supports(name, dialect.CapabilityForUpdate),
			dialect.Supports(name, dialect.CapabilitySkipLocked))
	}

	return nil
}
