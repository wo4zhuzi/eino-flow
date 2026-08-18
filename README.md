# eino-flow

基于 Eino 的通用 AI Workflow 编排层。仓库当前完成了第一个可运行闭环：文档索引工作流。

当前实现迁移自 `eino-lab` 的 demo19，源快照为 `31aba6b`。迁移过程中保留了已经验证的文档摄取、结构化解析、自动切分和模拟下游，同时抽取了仓库内部可复用的工作流运行层。

## 当前能力

| 阶段 | 状态 | 说明 |
|------|------|------|
| 文档摄取 | 真实 | 加载本地文件或受安全策略约束的 HTTP/HTTPS URL |
| 文档解析 | 真实 | Markdown 使用独立结构化 Parser，其他格式使用 ingestion 默认 Parser |
| 文档切分 | 真实 | 根据 Parser 输出能力选择 Structure-aware 或 Parent-child 策略 |
| Embedding | 模拟 | 不调用模型 |
| 索引持久化 | 模拟 | 不连接数据库 |
| 索引校验 | 模拟 | 不伪造校验成功 |
| 索引发布 | 模拟 | 不切换线上索引版本 |
| 结果构建 | 真实 | 返回 Parser、Chunk、关系、统计和各阶段状态 |

稳定工作流标识为 `rag_document_indexing@v4`，成功状态为 `chunked_with_simulated_downstream`。

PostgreSQL/GORM 连接基础设施、RAG 索引表 schema 校验、building Set 持久化，以及基于 Eino 官方 OpenAI 组件的 `text-embedding-v4` 客户端已经就绪，但尚未接入上述工作流；当前运行入口仍不会连接模型服务、数据库或执行索引写入。

Embedding 客户端固定请求 1536 维向量，并校验返回维度。阿里云兼容接口只返回请求级 Token 用量，因此当前按单条文本发起请求，以便为后续 `chunk_embeddings.embedding_token_count` 保存准确的服务端计量结果；`EMBEDDING_BATCH_SIZE` 在这一阶段控制最大并发数，合法范围为 1 到 10。

## 当前架构

```mermaid
flowchart TB
    Entry[cmd/rag-index-dev<br/>本地装配与运行入口]

    subgraph Runtime[通用工作流运行能力]
        Runner[Runner<br/>Compile once / Run many]
        Governance[Descriptor / RunID<br/>Observer / OperationError]
        Runner --- Governance
    end

    subgraph RAG[RAG 索引工作流]
        Ingest[ingest_document<br/>真实]
        Chunk[chunk_document<br/>真实]
        Embed[embed_chunks<br/>模拟]
        Persist[persist_index<br/>模拟]
        Validate[validate_index<br/>模拟]
        Publish[publish_index<br/>模拟]
        Result[build_result<br/>真实]

        Ingest --> Chunk --> Embed --> Persist --> Validate --> Publish --> Result
    end

    subgraph Packages[独立文档处理模块]
        Ingestion[eino-document-ingestion]
        Parser[eino-document-parser-structured]
        Chunking[eino-document-chunking]
    end

    Entry --> Runner --> Ingest
    Ingest --> Ingestion
    Ingestion --> Parser
    Chunk --> Chunking

    classDef real fill:#e8f5e9,stroke:#2e7d32,color:#1b1b1b;
    classDef simulated fill:#fff8e1,stroke:#f57f17,stroke-dasharray: 5 5,color:#1b1b1b;
    class Ingest,Chunk,Result real;
    class Embed,Persist,Validate,Publish simulated;
```

## 索引存储核心逻辑

以下是已经实现、但尚待真实工作流接入的索引构建与存储链路：

```mermaid
flowchart TB
    Spec[BuildSpec<br/>作用域、文档、Chunk 配置、模型配置]
    ChunkResult[Chunking Result<br/>Chunk 内容与关系]

    subgraph Module04[模块 04：入库前统一组装]
        MapBuild[MapBuild<br/>校验并规范化输入]
        BuildData[BuildData<br/>Set + Chunks + EmbeddingInputs]
        Hash[生成 model_key<br/>生成文本 input_sha256]

        MapBuild --> Hash --> BuildData
    end

    subgraph Module05[模块 05：PostgreSQL 持久化]
        Prepare[PrepareBuild 短事务<br/>写入 Set、对齐完整 Chunk 快照]
        Compare{相同 Set、Chunk、model_key 下<br/>input_sha256 是否一致}
        Reuse[复用已有向量<br/>不调用模型]
        Missing[返回缺失或变化的<br/>EmbeddingInput]
        Save[SaveEmbeddings 短事务<br/>Upsert 向量并设置 searchable=false]

        Prepare --> Compare
        Compare -->|一致| Reuse
        Compare -->|不存在或变化| Missing
    end

    Embed[Embedding Client<br/>事务外生成向量和 Token 数<br/>待模块 07 接入]

    subgraph PostgreSQL[PostgreSQL]
        Sets[(chunk_sets<br/>构建版本与状态)]
        Chunks[(chunks<br/>正文与关系)]
        Embeddings[(chunk_embeddings<br/>文本、Hash、向量)]
    end

    Spec --> MapBuild
    ChunkResult --> MapBuild
    BuildData --> Prepare
    Prepare --> Sets
    Prepare --> Chunks
    Embeddings --> Compare
    Missing --> Embed --> Save --> Embeddings
```

向量记录使用 `(chunk_set_id, chunk_id, model_key)` 标识一个 Chunk 在特定模型空间中的当前向量；`input_sha256` 是最终 Embedding 文本的 Hash，用于判断已有向量能否复用。模型或模型配置变化会生成新的 `model_key`，文本变化会生成新的 `input_sha256`，任一变化都需要重新生成向量。

`PrepareBuild` 和 `SaveEmbeddings` 分别使用短事务，Embedding 网络请求位于两个事务之间，避免模型调用长时间占用数据库锁。当前默认工作流仍使用模拟节点，模块 07 接入完成后才会实际执行这条链路。

### 文档身份与索引版本生命周期

`document_id` 表示一篇文章的稳定业务身份，`content_sha256` 表示该文章当前的内容版本，`chunk_set_id` 表示一次独立的索引构建。相同文章重新构建时保留 `document_id`，并创建新的 Set UUID。

```mermaid
flowchart TB
    Identity[可信文档身份<br/>tenant_id + knowledge_base_id + document_id]
    ContentA[文章内容版本 A<br/>content_sha256=A]
    ContentB[文章内容版本 B<br/>content_sha256=B 或与 A 相同]

    Identity --> SetA[Set A<br/>独立 chunk_set_id]
    Identity --> SetB[Set B<br/>新的 chunk_set_id]
    ContentA --> SetA
    ContentB --> SetB

    SetA --> ActiveA[Set A：active<br/>向量 searchable=true]
    SetB --> BuildingB[Set B：building<br/>向量 searchable=false]

    ActiveA --> Before[发布前召回<br/>只使用 Set A]
    BuildingB -. 不参与召回 .-> Before

    ActiveA --> Publish{Set B 校验通过<br/>发布事务原子切换}
    BuildingB --> Publish

    Publish --> RetiredA[Set A：retired<br/>向量 searchable=false]
    Publish --> ActiveB[Set B：active<br/>向量 searchable=true]
    ActiveB --> After[发布后召回<br/>只使用 Set B]
    RetiredA -. 不参与召回 .-> After
```

发布以完整 Set 为单位，不会只切换部分 Chunk。Retriever 必须在数据库查询阶段同时约束可信作用域、`chunk_sets.status='active'`、`chunk_embeddings.searchable=true` 和目标 `model_key`，避免 building 或 retired 版本进入向量候选集。模块 05 已实现 building Set 写入，模块 06 已实现事务内完整性校验、作用域 advisory lock 和无空窗原子发布；真实工作流接入属于模块 07，尚未完成。

`internal/workflow` 只管理工作流运行，不依赖 RAG 业务类型：

- `Descriptor`：稳定的工作流名称和定义版本。
- `RunID`：由调用入口提供的一次执行标识。
- `Runner`：启动时编译一次，并发复用已编译拓扑。
- `Observer`：把日志、Trace 或指标适配为 Eino Callback。
- `OperationError`：补充工作流、版本、RunID 和生命周期阶段，同时保留原始错误链。
- `RunOption`：只暴露受控的 Observer 和最大步数等运行能力。

## 目录结构

```text
.
├── cmd/rag-index-dev/          本地运行与 Eino Dev 入口
├── governance/                仓库治理规范
├── internal/config/           全局运行配置、校验与脱敏
├── internal/embedding/        text-embedding-v4 客户端与精确 Token 计量
├── internal/postgres/         PostgreSQL 通用连接与生命周期
├── internal/workflow/         通用工作流运行能力
├── internal/rag/indexing/     RAG 文档索引 Feature
├── internal/rag/indexstore/   RAG 索引存储边界
│   └── postgres/              表映射、schema 校验与构建持久化
├── testdata/knowledge.md      默认测试语料
├── docs/plans/                后续演进路线
├── AGENTS.md                  仓库治理规则路由
├── go.mod
└── go.sum
```

代码按 Feature 组织：`internal/rag/indexing` 独立拥有自己的 Request、Result、Dependencies、状态和拓扑；`internal/rag/indexstore/postgres` 持有索引构建和未来检索共同依赖的 PostgreSQL 表映射与存储实现。跨业务域的稳定技术能力放在 `internal/<capability>`，RAG 域内的稳定共享能力放在 `internal/rag/<capability>`。`internal/workflow` 不知道文档、Chunk 或索引阶段，`internal/config` 只负责运行配置边界，业务包不直接读取配置源。

详细的目录归属、依赖方向和 Go 编码约定见 [Go 工程规范](governance/go-engineering.md)。

## 运行

仓库使用 Go `1.26.x`、Eino `v0.9.12`。默认示例不需要模型、数据库或 API Key。

运行默认 Markdown：

```bash
go run ./cmd/rag-index-dev
```

解析指定文件或 URL：

```bash
go run ./cmd/rag-index-dev /absolute/path/to/document.txt
go run ./cmd/rag-index-dev https://example.com/document.md
```

输出包含完整 Chunk 正文，适合本地开发和测试，不应直接作为生产日志格式。

## Eino Dev

```bash
EINO_DEV=true go run ./cmd/rag-index-dev
```

在 Eino Dev 中连接 `127.0.0.1:52538`，选择 `rag_document_indexing@v4`。该模式只用于本地调试，不应暴露到公网。

## 验证

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

测试覆盖 Markdown Structure-aware、TXT Parent-child、稳定 ID、语义路径、输入不可变性、错误链、超大原子块和公共 Runner 并发复用。

## 后续演进

真实索引下游的入库边界、表结构、写入校验和发布流程见 [RAG 索引入库设计 V1](docs/designs/rag-index-storage-v1.md)。其余后续方向维护在 [演进计划](docs/plans/2026-08-11-demo19-rag-migration.md)中。每个里程碑开始前先单独完成设计讨论和决策确认，再进入实现；当前不会提前实现真实索引下游或查询工作流。
