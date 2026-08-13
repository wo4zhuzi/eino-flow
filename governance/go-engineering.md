# Go 工程规范

本文档适用于仓库内所有 Go 源码、测试、依赖、package 和 Go 项目目录结构变更。

## 1. 核心原则

- 遵循 Go idiomatic 风格：显式处理错误，优先使用简单结构、隐式接口实现和标准库；在保证正确性、安全性和边界完整的前提下，保持代码清晰、可测试且易于长期维护。
- 业务代码采用 Package by Feature，首先按业务能力组织，而不是在顶层按技术层拆分。
- 只为当前真实需求设计，不提前实现尚不存在的扩展点、兼容层或通用框架。
- 保持 package 职责单一、名称具体，禁止创建含义模糊的 `common`、`utils`、`base` 或万能 `platform` 包。
- 遵循最小变更原则。不要借业务改动进行无关重构、目录重排或批量改名。

## 2. 目录与 Package by Feature

当前推荐结构：

```text
cmd/                              程序入口与依赖组装
internal/config/                  全局配置加载、规范化与校验
internal/workflow/                跨 Feature 的工作流运行能力
internal/postgres/                通用数据库连接、健康检查与生命周期
internal/embedding/               可跨 Feature 复用的 Embedding 客户端
internal/rag/indexing/            RAG 索引 Feature
internal/rag/retrieval/           RAG 检索 Feature
internal/rag/indexstore/          索引存储的稳定领域类型（存在真实共享需求时）
internal/rag/indexstore/postgres/ RAG Index Store 的 PostgreSQL 实现
```

目录归属规则：

- 业务类型、规则、流程和使用方接口放在对应 Feature 内。
- 只服务一个 Feature 的数据库映射、SQL、第三方适配器放在该 Feature 内。
- 跨多个 Feature 且职责稳定的技术能力，才允许放到 `internal/<capability>`。
- `internal/postgres` 只管理通用连接能力，不包含 RAG 表结构、业务 SQL 或发布规则。
- `internal/rag/indexing` 负责索引构建业务流程，`internal/rag/retrieval` 负责查询与召回业务流程。
- 索引表映射、schema 校验、事务写入、发布和查询 SQL 属于 `internal/rag/indexstore/postgres`；该包可以同时实现索引与检索使用方定义的接口。
- `internal/rag/indexstore` 只保存索引存储真正共享且语义稳定的领域类型；没有共享需求时不为目录完整性创建空 package。
- GORM 表模型是 PostgreSQL 适配器的私有实现细节。查询可定义私有 Row/Projection 类型，不要求复用写入模型，也不得让业务 Feature 依赖 ORM 类型。
- 不在顶层建立统一的 `handler/`、`service/`、`repository/`，避免横向拆散业务。
- 不为形式完整强制每个 Feature 创建 handler、service、repository 等空层。只有职责和复杂度真实存在时才在 Feature 内拆分。

## 3. 职责边界

- `handler` 专指 HTTP、gRPC 等传输层：负责协议解析、参数转换、调用 use case，以及将结果映射为协议响应。
- Eino 工作流中的步骤称为 `node`，相关文件和类型使用 `node` 命名，不使用 `handler`，避免与传输层混淆。
- `service` 或 `usecase` 负责业务流程编排和业务规则，不处理具体传输协议。
- `repository` 或 `store` 表达业务需要的数据访问语义，不暴露 GORM、SQL Row 等基础设施细节。
- 基础设施实现负责数据库、缓存、消息队列和第三方客户端等外部依赖适配。
- `main.go` 负责加载配置、初始化资源、显式组装依赖、启动程序和释放资源，不承载业务规则。

## 4. 依赖方向

依赖必须保持单向：

```text
cmd -> Feature -> 使用方接口
cmd -> 基础设施实现 -> Feature 接口和领域类型
```

- 业务 Feature 不得直接初始化数据库、缓存、消息队列或第三方 SDK 客户端。
- 业务 Feature 不得读取环境变量或配置文件。
- 业务代码不得依赖具体数据库 Driver、GORM、OpenAI SDK 等实现细节。
- 位于 Feature 内的基础设施实现包可以依赖相应 SDK，并实现 Feature 定义的接口。
- 同时服务多个 RAG Feature 的 Index Store 适配器放在 `internal/rag/indexstore/<technology>`，不得归属于任一单独使用方。
- Feature 之间通过明确的类型或小接口协作，禁止通过全局状态、隐式注册或万能共享包耦合。
- 发现循环依赖时应重新审视职责归属，不得通过复制类型或建立 `common` 包掩盖问题。

## 5. Interface 与依赖注入

- Interface 定义在使用方，而不是实现方。
- Interface 应保持最小，只包含当前调用方实际需要的方法。
- 仅在需要替换实现、隔离外部依赖或方便测试时定义 Interface。
- 不为每个 struct 创建一一对应的 Interface，不使用 Java 风格的 `XxxInterface`、`XxxImpl` 或 `BaseXxx` 命名。
- 构造函数显式接收必需依赖，并优先返回具体类型。
- 构造函数应校验不可缺少的依赖；禁止让对象以部分初始化状态运行。
- 禁止使用全局变量保存数据库连接、缓存客户端、SDK Client 等有状态依赖。
- 具体实现可以使用编译期断言验证接口兼容性：

```go
var _ indexing.Store = (*Store)(nil)
```

## 6. 配置与敏感信息

- 配置只在启动装配层读取一次，经过规范化和完整校验后再传递给构造函数。
- 业务模块只接收强类型配置值或已经构造完成的依赖，不感知配置来源。
- 普通配置可以来自环境变量，后续存在实际需求时可增加 YAML 或 JSON 配置源。
- `.env` 只用于本地开发，不得提交仓库，也不得作为生产环境唯一的秘密管理方式。
- 生产密码、Token 和 API Key 优先通过 Secret 文件、systemd credentials 或 Secret Manager 提供。
- 密码、Token、API Key 不得进入日志、错误信息、测试快照、Trace、指标标签或默认格式化输出。
- 多种配置源并存时必须明确覆盖顺序和冲突规则，禁止依赖隐式优先级。
- 配置错误应指出字段或变量名，但不得包含对应的敏感原值。

## 7. Go 编码规范

- package 名使用简短、具体的小写单词，避免重复上下文，例如 `indexing.Store`，而不是 `indexing.IndexingStore`。
- 导出标识符必须有以标识符名称开头或语义明确的 Go Doc 注释。
- 注释重点解释约束、原因和不明显的设计决策，不逐行复述代码。
- `context.Context` 作为需要它的函数第一个参数传入，不保存到长期对象字段中，也不传递 `nil`。
- 错误使用 `%w` 保留错误链；调用方需要分类处理时，提供稳定的哨兵错误或具体错误类型。
- 错误信息应包含必要操作上下文，但不得重复包装相同语义或泄漏敏感数据。
- 优先使用早返回减少嵌套；避免过长函数和同时承担多个职责的类型。
- 除非零值确实可用，否则通过构造函数建立对象不变量。
- 不使用 `init()` 执行业务注册、连接外部服务或创建有状态资源。
- 不隐藏 I/O、网络调用和事务边界；相关行为必须能从接口和调用链中识别。

## 8. 数据与事务

- Repository 或 Store 接口表达业务操作，不直接照搬数据库表 CRUD。
- 事务边界由完成业务原子操作的一方控制，不在调用链中隐式开启多个事务。
- 事务内的所有数据访问必须显式使用同一个事务句柄，禁止事务逃逸。
- 不使用 GORM `AutoMigrate` 修改生产表；数据库结构变更使用可审计的迁移或明确 DDL。
- SQL 必须参数化。表名、列名等不能参数化的标识符必须来自受控常量或经过严格校验的配置。
- 数据库模型与业务类型只在语义一致时复用；不得为了减少转换让业务层依赖 ORM 标签和 Driver 类型。

## 9. 测试与验证

- 测试规模应与变更风险匹配；业务规则、错误语义、边界条件和安全约束必须有测试。
- 优先使用表驱动测试，测试名称应描述行为和场景。
- 使用调用方定义的小接口注入替身，不引入只为测试服务的生产抽象。
- 单元测试不得依赖本地 `.env`、真实数据库、真实外部 API 或执行顺序。
- 外部依赖集成测试必须显式开启、隔离数据，并保证失败信息不包含秘密。
- 涉及并发、共享 Runner、连接池或缓存状态时必须执行竞态检测。
- 修改 Go 代码后至少执行：

```bash
gofmt -w <修改的 Go 文件>
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
git diff --check
```

- 如果因环境限制无法执行某项验证，必须在交付说明中明确指出，不得默认视为通过。

## 10. 变更决策检查

新增 package、Interface 或抽象层之前，依次确认：

1. 它解决的是当前已经存在的问题，而不是推测中的未来需求。
2. 它有单一、具体且可描述的职责。
3. 它的依赖方向符合 Feature 边界，不会引入循环依赖。
4. 它能减少真实复杂度、隔离外部依赖或显著改善测试，而不是只增加转发。
5. 使用者能够从 package 名、类型名和构造函数看出正确用法。

任一项无法确认时，优先保留更直接的实现，待真实需求出现后再抽取。
