package academy

import (
	"github.com/tmoeish/tsq/v5"
)

// 这个文件是初始化顺序的回归门，**文件名必须继续排在 `course.tsq.go` 之前**。
//
// 下面的查询变量只引用单独的列变量，从不提到 `Course__Cols`。Go 的包级初始化顺序只看
// 初始化表达式里出现的引用。生成代码把表声明成 `tsqCourseTable`（句柄）→ 列 →
// `TableCourse = tsqCourseTable.Define(...)`，`TableCourse` 的初始化表达式列出了全部列，
// 所以这个引用了 `TableCourse` 的查询一定排在表定义完成之后。
//
// 生成代码如果让查询直接引用句柄，或者把列登记挪进 init 函数，这里会在包初始化时
// panic（"used before Define"），三个示例程序全都跑不起来。

// QueryCourseTitles lists course identifiers and titles without selecting every
// column, so it depends only on the individual column variables it names.
var QueryCourseTitles = tsq.
	Select(Course_ID, Course_Title).
	From(TableCourse).
	Where(Course_Published.EQ(Course_Published.Param())).
	OrderBy(Course_ID.Asc()).
	MustBuild()
