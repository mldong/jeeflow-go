package spi

import (
	"context"
	"time"

	"github.com/mldong/jeeflow-go/model"
)

// ProcessRepository 流程仓储接口（方法名对齐 Java/Node camelCase）
type ProcessRepository interface {
	FindDefineByID(ctx context.Context, id int64) (*model.ProcessDefine, error)
	// FindDefineByName 按流程编码查最新一条定义（v1.1.0，Facade deploy 版本管理用）
	FindDefineByName(ctx context.Context, name string) (*model.ProcessDefine, error)
	// 定义写操作（v1.0.1，集成反馈①）：保存/更新/启停/删除流程定义
	SaveDefine(ctx context.Context, def *model.ProcessDefine) error
	UpdateDefine(ctx context.Context, def *model.ProcessDefine) error
	UpdateDefineState(ctx context.Context, defineID int64, state int) error
	RemoveDefine(ctx context.Context, defineID int64) error

	FindInstanceByID(ctx context.Context, id int64) (*model.ProcessInstance, error)
	SaveInstance(ctx context.Context, inst *model.ProcessInstance) error
	UpdateInstance(ctx context.Context, inst *model.ProcessInstance) error

	FindTaskByID(ctx context.Context, taskID int64) (*model.ProcessTask, error)
	SaveTask(ctx context.Context, task *model.ProcessTask) error
	UpdateTask(ctx context.Context, task *model.ProcessTask) error
	FindDoingTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindDoneTasks(ctx context.Context, instanceID int64, taskNames []string) ([]*model.ProcessTask, error)
	FindHistoryTasks(ctx context.Context, instanceID int64) ([]*model.ProcessTask, error)

	FindTaskActors(ctx context.Context, taskID int64) ([]string, error)

	// AddTaskActor 追加任务参与者（逐人一行；**追加语义**，不清空原参与者，issues/03）。
	//
	// issues/142 B 批「归属值写侧归一」（issues/142 B 批 · spec 06-facade.md §2.11，
	// 判据逐字承 issues/141 G10 · spec §2.10）——**空值义务同样落在实现方自己身上**，
	// 不只是引擎/门面的入参解析那一层：
	//   - 入参 actors 里的**空串、纯空白一律丢弃**，落库值取 trim 后的串
	//     （" 123 " 与 "123" 是同一个人，不 trim 会让同一人落两行、把判重打穿）；
	//   - 同一次调用内的重复折叠；
	//   - 判据的单点＝[NormalizeActors]，本仓内存仓与 JDBC 仓都调它，
	//     第三方仓储实现请同样复用它而不是各写一份（两份判据迟早分叉）；
	//   - ⚠️ 反向哨兵：只吃空值，"0" 这类"看起来像空"的正常 id **不得**被丢掉，
	//     "0" 与 "00" 是两个不同的人（比较用字符串等值，严禁借语言自带的假值判据）；
	//   - **主键另判一档**：taskID 是主键不是归属值，缺失/空/0 必须由**调用方**响亮报错，
	//     不得拿 空串/0 当 id 往下落库（§2.11「主键类参数另判一档」）。
	//
	// 为什么义务要写在实现方身上：绕过引擎/门面直连仓储的调用方（集成层、第三方仓储消费者）
	// 同样不得把空归属值灌进 wf_process_task_actor.actor_id——那是 issues/129 那族
	// "空 operator 读全库"的上游进水口（spec §2.11 硬要求①「两层都挡」）。
	AddTaskActor(ctx context.Context, taskID int64, actors []string) error

	// RemoveTaskActor 移除任务参与者（摘人／转办摘原人／门面 processTask/removeTaskActor 都落这一支）。
	//
	// **归属值删除腿义务**（issues/137 §3-6 · spec 06-facade.md §processTask/removeTaskActor 语义 6
	// ＋ §2.11 写点表末行，owner 2026-10-02 拍「两形并集」）——**与上面 [ProcessRepository.AddTaskActor]
	// 的写侧义务不同，别照抄**（写侧只落 trim 形；删除腿要「原值 ∪ trim 值」两形）：
	//  1. **空值一律丢弃、不参与匹配**：nil／空串／纯空白都不得进 DELETE，否则历史 actor_id 空串
	//     脏行会被批量误删（那是替脏数据做掉唯一痕迹）；
	//  2. **非空值同时以「原值」与「trim 值」两形匹配**（按字面去重、保序）。只取 trim 形 ⇒ 门面按
	//     语义 6 交出的历史脏行原值 " 9101 " 被削成 9101，真库 NO PAD 排序规则下那一行删不掉而门面
	//     报成功（**假成功**：被摘的人待办还在）；只取原值 ⇒ 绕过门面直连仓储的调用方传 " 8601 "
	//     时删不掉写侧归一后落库的规范行 8601（issues/142 §9.2 那一路）。两形并集同时满足两侧，
	//     且按 §2.11 归一口径 " 9101 " 与 9101 本就是同一个人，两行都删才是"摘掉这个人"的正确结果，
	//     不构成误删；
	//  3. **展开后为空 ⇒ 早退，一条 DELETE 都不发**——空并集不得退化成"清空该任务全部参与者"
	//     （那是语义 5「至少需保留一名参与人」的仓储侧对偶）。
	//
	// 判据本体只有一枚＝[ActorDeleteForms]（八栈同名件，java StringUtils.actorDeleteForms／
	// node spi.actorDeleteForms／py spi.actor_delete_forms／php CcActorUtil::deleteForms／
	// rs model::actor_delete_forms／moon @model.actor_delete_forms／c# PageQuery.ActorDeleteForms），
	// trim 与判空仍复用 [NormalizeActors] 那一枚，**不要在仓储里抄第二份**（两份判据迟早分叉）。
	// ⚠️ 反向哨兵：判空一律 trim(x) == ""，"0" 是合法 id 必须留下，"0" 与 "00" 是两个不同的人
	// （严禁借语言自带的假值判据）。去重按**字面**做，" 9101 " 与 "  9101  " 是两种不同的原值形，都要保留。
	// ⚠️ nil 元素由**调用方**在形态拆解那一层就丢掉，严禁先 fmt.Sprint 成 "<nil>" 再传进来。
	//
	// SQL 仓与内存仓在同一条判据上必须给同一个答案（issues/117 场景 27 那把尺子）。
	// **主键另判一档**：taskID 是主键不是归属值，缺失/空/0 由**调用方**响亮报错，同 AddTaskActor 末段。
	RemoveTaskActor(ctx context.Context, taskID int64, actors []string) error

	// CreateCcInstance 建 cc 行（逐抄送人一行）。
	//
	// issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：同一 (instanceID, actorID) 已有 cc 行时
	// **跳过**——①不新增行 ②不重置未读状态（state）③不更新原行时间（create_time/update_time
	// 逐字不变，也没有"删旧插新"），重复抄送同一个人在数据面上是 no-op。同一次调用里重复给的
	// 同一个人也折叠成一行。判重在**写侧**：查询侧不引入 DISTINCT，历史重复行不清理。
	//
	// ⚠️ 需要"实际新建了谁"的调用方（引擎/门面的三条抄送入口）一律改用
	// [ProcessRepository.CreateCcInstanceIfAbsent]，拿返回的子集去 fire CC_CREATE。
	//
	// issues/141 G10「空抄送人不建 cc 行」（spec 06 §2.10）：入参里的**空串、纯空白一律丢弃**，
	// 落库值取 trim 后的串（" 123 " 与 "123" 是同一个人）。这条义务要落在**实现方自己身上**
	// 而不只落在引擎漏斗里——绕过引擎/门面直连仓储的调用方同样不得把空归属值灌进 actor_id
	// （issues/129 那族"空 operator 读全库"的病根）。判据的单点＝[NormalizeActors]
	// （cc 侧旧名 [NormalizeCcActors] 只是它的转发，不是第二份判据），
	// 本仓内存仓与 JDBC 仓都调它，第三方仓储实现请同样复用它而不是各写一份。
	CreateCcInstance(ctx context.Context, instanceID int64, creator string, actorIDs ...string) error

	// FindCcActorIDs 读某实例**已存在**的 cc 行 actor id（issues/141 G2 写侧判重的读侧，
	// 对齐 Java IProcessRepository.findCcActorIds）：供建 cc 的三条入口（发起 f_ccActors／
	// 办理 tf_ccActors／门面手动 createCCInstance）判重用。返回值来自真实行集（内存仓的行、
	// SQL 仓的 SELECT），不得是内存猜测；顺序按建行顺序（SQL 仓 ORDER BY id）。
	FindCcActorIDs(ctx context.Context, instanceID int64) ([]string, error)

	// CreateCcInstanceIfAbsent 写侧幂等建 cc 行，返回**实际新建**的 actor 子集
	// （issues/141 G2，对齐 Java IProcessRepository.createCcInstanceIfAbsent）。
	//
	// 子集形状：跳过 [ProcessRepository.FindCcActorIDs] 已有的 actor，顺序与入参一致，
	// 同一次调用内的重复也折叠；全部已存在时返回空子集（不是 nil 语义上的"全部"）。
	//
	// 为什么返回子集而不是 error-only：spec 11.2 原则 1「码值表达发生了什么事实」⇒
	// 没发生"创建"就不得 fire CC_CREATE（码 4）。三条入口逐人 fire 的入参一律换成这个子集，
	// **子集为空则整支不 fire**（不空转、也不照旧按原始请求全量 fire）。
	CreateCcInstanceIfAbsent(ctx context.Context, instanceID int64, creator string, actorIDs ...string) ([]string, error)

	// UpdateCcStatus 抄送已读（state 0→1）。
	//
	// issues/142 B 批（spec 06-facade.md §2.11 写点表「processInstance/updateCCStatus 的 operator」
	// 一行）：**入参归一后再比**——actorID 取 trim 后的值，空串/纯空白按"没给归属"处理，
	// 严禁退化成"这条条件不加"把 state=1 打到历史 actor_id 为空串 的脏行上（issues/129 那族病根）。
	// 判据的单点＝[NormalizeActors]；门面腿已在解析时归一，仓储这层是第二道（§2.11 硬要求①）。
	UpdateCcStatus(ctx context.Context, instanceID int64, actorID string) error

	// PageCcInstances 我的抄送分页（v1.3.0，对齐 Java pageCcInstances）：
	// 按抄送人 actorID 过滤实例列表，返回行数据（含关联定义名/版本）+ 总数。
	//
	// **归属条件必填**（issues/141 G1 · spec 06 §2.5）：本入口的取数范围必须由 cc.actor_id
	// 的**有效**归属条件圈定，条件缺失或为空值时**返回空页**（total=0、rows 为空集），
	// 严禁退化成"这条条件不加"而返回全部实例。
	//   - 本栈归属有两个载体：显式入参 actorID（主通道，门面 ccList 恒挂 operator）＋
	//     query.Conditions 里的 cc.actor_id 条件（m_ 参数通道）。actorID 为空值形态、
	//     或任一 cc.actor_id 条件为空值形态 ⇒ 都按"没给归属"处理 ⇒ 空页。
	//   - "有效"判据＝值非 nil、字符串 TrimSpace 后非空、集合非空（空 IN 即"没有人"）。
	//   - **非归属列**的空值放行不变：m_LIKE_* 之类可选过滤传空串仍按"没填"忽略。
	//   - SQL 仓与内存仓必须给同一个答案（issues/117 场景 27 那把尺子扩到 ccList）。
	PageCcInstances(ctx context.Context, query PageQuery, actorID string) ([]*model.CcInstanceRow, int, error)

	// ── 核心表分页（v1.5.0，对齐 Java pageDefines/pageInstances/pageTodoTasks/pageDoneTasks）──

	// PageDefines 流程定义分页
	PageDefines(ctx context.Context, query PageQuery) ([]*model.DefineRow, int, error)
	// PageInstances 我发起的流程实例分页（operator 过滤）
	PageInstances(ctx context.Context, query PageQuery, operator string) ([]*model.InstanceRow, int, error)
	// PageTodoTasks 我的待办分页（actorID 过滤，仅进行中任务）
	PageTodoTasks(ctx context.Context, query PageQuery, actorID string) ([]*model.TaskRow, int, error)
	// PageDoneTasks 我的已办分页（operator 过滤，非进行中任务）
	PageDoneTasks(ctx context.Context, query PageQuery, operator string) ([]*model.TaskRow, int, error)

	// ── 统计（v1.8.25，issues/103，对齐 Java stats SPI 9 方法）──

	// QueryInstancesForStats 查询实例列表（轻量级，不加载关联任务）。stateIn/timeField+start/end 均可空
	QueryInstancesForStats(ctx context.Context, stateIn []int, timeField string, start, end *time.Time) ([]model.InstanceStatsRow, error)
	// QueryTasksForStats 查询任务列表。state/start/end 均可空
	QueryTasksForStats(ctx context.Context, state *int, start, end *time.Time) ([]model.TaskStatsRow, error)
	// StatsAvgCompletedDurationSeconds 已完成实例平均耗时（秒）
	StatsAvgCompletedDurationSeconds(ctx context.Context, start, end *time.Time) (int, error)
	// StatsPendingAndOverdueCount 待办数 + 逾期数
	StatsPendingAndOverdueCount(ctx context.Context) (pending int, overdue int, err error)
	// StatsCompletedTaskAggregate 已完成任务聚合：total, countersign, onTime, onTimeDenom
	StatsCompletedTaskAggregate(ctx context.Context) (total, countersign, onTime, onTimeDenom int, err error)
	// StatsStuckNodeGroup 卡滞节点分组
	StatsStuckNodeGroup(ctx context.Context, limit int) ([]map[string]interface{}, error)
	// StatsStuckApproverGroup 卡滞审批人分组
	StatsStuckApproverGroup(ctx context.Context, limit int) ([]map[string]interface{}, error)
	// StatsDefineGroup 流程定义分组
	StatsDefineGroup(ctx context.Context, start, end *time.Time, limit int) ([]map[string]interface{}, error)
	// StatsCompletedInstanceDurations 已完成实例耗时列表（秒）
	StatsCompletedInstanceDurations(ctx context.Context, start, end *time.Time) ([]int, error)
}

// UserProvider 用户信息提供者
type UserProvider interface {
	GetUser(userID string) (*model.UserInfo, error)
}

// OrgUserProvider 组织维度用户提供者（issues/16）——部门领导 / 部门分管领导 / 角色成员。
// 通用业务语义，业务方只实现数据接口，不写 AssignmentHandler。
type OrgUserProvider interface {
	// FindDeptLeaders 部门领导（deptId → 领导 userId 列表）
	FindDeptLeaders(deptID string) ([]string, error)
	// FindDeptMainLeaders 部门分管领导（deptId → 分管领导 userId 列表）
	FindDeptMainLeaders(deptID string) ([]string, error)
	// FindByRole 按角色取人（roleCode → userId 列表）
	FindByRole(roleCode string) ([]string, error)
}

// IDGenerator ID 生成器
type IDGenerator interface {
	NextID() int64
}

// ExpressionEvaluator 表达式求值器
type ExpressionEvaluator interface {
	Eval(expr string, vars map[string]interface{}) (interface{}, error)
}
