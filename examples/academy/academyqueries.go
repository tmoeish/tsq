package academy

import (
	"github.com/tmoeish/tsq/v4"
)

// 这个文件是初始化顺序的回归门，**文件名必须继续排在 `course.tsq.go` 之前**。
//
// 下面的查询变量只引用单独的列变量，从不提到 `Course__Cols`。Go 的包级初始化顺序只看
// 初始化表达式里出现的引用，而 `Cols()` 是通过接口方法在运行期才去取那个切片的，依赖
// 分析看不见。真正让顺序成立的是生成代码里的
// `var TableCourse tsq.Table = tsq.TableWithCols(Course{}, Course__Cols)`：
// 这个查询引用了 `TableCourse`，于是传递地依赖上了列切片。
//
// 生成代码退回 `var TableCourse tsq.Table = Course{}` 的话，这里会在包初始化时 panic，
// 三个示例程序全都跑不起来。把这个文件改名到 `course.tsq.go` 之后就等于把这道门关掉了。

// QueryCourseTitles lists course identifiers and titles without selecting every
// column, so it depends only on the individual column variables it names.
var QueryCourseTitles = tsq.
	Select(Course_ID, Course_Title).
	From(TableCourse).
	Where(Course_Published.EQVar()).
	OrderBy(Course_ID.Asc()).
	MustBuild()
