# RAGFlow 问答（Q&A）切片过程详解

> 本文基于当前 Go 实现（`internal/ingestion/`）整理，覆盖从文件解析到问答对生成、切片入库的完整链路，并逐路径举例说明。旧 Python 实现（`rag/app/qa.py`）已删除，Go 实现中的多处注释仍以其行为为对齐基准。

---

## 1. 总览：QA 切片在流水线中的位置

数据集选择 `parser_id = "qa"` 时，使用内置模板 [ingestion_pipeline_qa.json](internal/ingestion/pipeline/template/ingestion_pipeline_qa.json)，DSL 图为四节点流水线：

```
File ──► Parser ──► QAChunker ──► Tokenizer(Indexer)
(读文件)   (解析为      (提取问答对,     (分词 + 写索引,
           结构化文档)   每对一个切片)    text→content_with_weight)
```

- 模板注册与排序：[builtin_registry.go:23](internal/ingestion/pipeline/builtin_registry.go#L23)（`"qa": 1`，紧跟 `general` 之后）。
- 模板定位（官方描述）：适用于 FAQ 页面、访谈记录、以问答格式组织的知识库。
- 核心组件实现：[qa.go](internal/ingestion/component/chunker/qa.go)，组件名 `QAChunker`。

**一句话概括**：QAChunker 把上游 Parser 输出的结构化文档，按"问题 / 答案"的天然边界（分隔符、标题、表格行）拆成问答对，**每对一个切片**，切片文本统一为 `问题：{q}\t回答：{a}`（或英文 `Question: {q}\tAnswer: {a}`），再交给 Tokenizer 建立全文检索与向量索引。

---

## 2. Parser 阶段：统一的结构化输出

Parser 组件无论处理什么文件（pdf/docx/xlsx/md/txt/csv…），都通过 [buildParserOutputs](internal/ingestion/component/parser_dispatch.go#L311) 输出统一的 payload：

```json
{
  "name": "faq.pdf",
  "output_format": "json",
  "json": [ ...类型化 DocItem 列表... ]
}
```

QAChunker 侧由 [decodeChunkerFromUpstream](internal/ingestion/component/chunker/token.go#L247) 解码为 `ChunkerFromUpstream`，其 `OutputFormat` 字段决定 QA 走哪条提取路径。虽然 Parser 通常输出 `json`，但手工编排的流水线（或测试）可以直接给出 `text` / `markdown` / `html` 载荷。

---

## 3. QAChunker：四条提取路径

分发逻辑在 [qa.go:113-127](internal/ingestion/component/chunker/qa.go#L113-L127)：

| 上游 `output_format` | 触发条件 | 提取函数 | 问答对来源 |
|---|---|---|---|
| `html` | HTML 载荷 | `extractQATable` | `<table>` 每行的前两个单元格 |
| `markdown` | Markdown 载荷 | `extractQAMarkdown` | 标题 = 问题，标题下正文 = 答案 |
| `text` | 纯文本（txt/csv） | `extractQAText` | 分隔符（tab 或逗号）拆两列 |
| `json`（默认） | 其余全部（pdf/docx/xlsx…） | `extractQAJSON` | 逐 item 判断：表格行走行、否则当文本 |

### 3.1 路径一：Text / CSV —— 分隔符拆分

入口 [extractQAText](internal/ingestion/component/chunker/qa.go#L279)，先由 [detectDelimiter](internal/ingestion/component/chunker/qa.go#L387) 探测分隔符：**统计全文中"恰好拆成两段"的行数，tab 不少于逗号即选 tab**（`tab >= comma`），否则走 CSV。

**例 1：tab 分隔的 txt**

输入 `test.txt`：
```
What is Go?\tGo is a programming language.
```
产出 1 个切片：
```
Question: What is Go?⇥Answer: Go is a programming language.
```
（⇥ 表示真实的 `\t`；此处组件参数 `lang=english`）

**例 2：逗号分隔的 CSV（带跨行引号字段）**

`csv.Reader` 以全文为输入（`LazyQuotes`、`FieldsPerRecord=-1`），因此引号内含换行的字段也能正确还原——这对齐了旧 Python 的 infiniflow/ragflow#16881 修复。实现见 [extractQATextCSV](internal/ingestion/component/chunker/qa.go#L331)，它用 `InputOffset()` 把"逻辑记录"映射回"物理行"，保证坏行能追加正确的原始文本。

```
"What is the refund policy?","It takes 7 days.
No restocking fee."
"What about exchanges?","Allowed within 30 days."
```
产出 2 个切片，第一条的 Answer 是完整两行文本。

**例 3：列数不对的行 → 并入上一条的答案**

```
Q1\tA1
这是一段补充说明，不属于任何问题
Q2\tA2
```
"补充说明"行拆不出两段，于是**追加到上一条答案**：
```
问题：Q1⇥回答：A1
这是段补充说明，不属于任何问题
问题：Q2⇥回答：A2
```

### 3.2 路径二：Markdown —— 标题层级切分

入口 [extractQAMarkdown](internal/ingestion/component/chunker/qa.go#L222)。核心规则：

- 维护一个 **标题栈**（questionStack + levelStack）。遇到 `#`~`######` 标题时，先把栈中层级 **不小于** 当前层级的旧标题弹出，再压入新标题；
- 遇到下一个标题（或文末）时 `flushAnswer`：**栈内标题按层级拼接（`\n` 连接）作为问题，标题之间的正文作为答案**；
- 代码块（``` 围栏）中的 `#` 不当作标题，原样进入答案。

**例 4：多级标题合成问题**

```markdown
# 退款政策
## 时效
购买后 7 天内可退。

## 费用
无手续费。
```

产出 2 个切片：

| 问题 | 答案 |
|---|---|
| `退款政策\n时效` | `购买后 7 天内可退。` |
| `退款政策\n费用` | `无手续费。` |

父标题自动成为子标题问题的前缀（多级上下文），同级标题互不携带。

**例 5：Markdown 答案渲染为 HTML**

Markdown 路径产出的答案会经 [renderMarkdown](internal/ingestion/component/chunker/qa.go#L174)（gomarkdown，启用表格扩展）转成 HTML 后再拼入切片文本，保证答案里的列表/表格/代码块在检索回显时结构不丢。

### 3.3 路径三：HTML 表格 —— 前两格配对

入口 [extractQATable](internal/ingestion/component/chunker/qa.go#L464)：把 HTML 表格标记解析为结构化 `TableData`，再按行取格。

**例 6：普通表格（xlsx 导出的 html 载荷）**

```html
<table><tr><td>Q1</td><td>A1</td></tr><tr><td>Q2</td><td>A2</td></tr></table>
```
产出 2 个切片：`问题：Q1⇥回答：A1`、`问题：Q2⇥回答：A2`。

**例 7：CSV 严格两列契约 vs 普通表格取前两非空格**

- **文件名是 `.csv`**（[qa.go:115](internal/ingestion/component/chunker/qa.go#L115) 传入 `strictPairs=true`）：一行必须**恰好两个单元格**，否则整行并入上一条答案（对齐旧 `qa.py:365`）。
  ```html
  <table><tr><td>question</td><td></td><td>extra</td></tr></table>   <!-- .csv 名 -->
  ```
  该行 3 格 ≠ 2 格 → 不产出切片（若前面已有问答对，则 `question,,extra` 追加到上一条答案）。
- **非 csv 名**（如 `.xls`）：取该行**前两个非空单元格**。
  同样的表格产出：`问题：question⇥回答：extra`（空格被跳过，extra 成为答案）。

### 3.4 路径四：JSON —— 逐 item 智能路由（pdf/docx/xlsx 等的主路径）

入口 [extractQAJSON](internal/ingestion/component/chunker/qa.go#L407)。对每个结构化 item：

1. 取其结构化表格数据 `TableData` 与纯文本；
2. **不看 `doc_type` 标签，看"行遍历器能否读出行"**：能读出行 → 走表格配对；读不出 → 当纯文本走 3.1 的分隔符路径。
   这样设计的原因：opendataloader / tcadp / somark 等 PDF 解析器会把不含 `<table>` 标记的文本错标成 `table` 类型；若按标签路由，这类 item 会进入表格提取器却读不到行，**所有问答对静默丢失**。
3. 每个提取出的问答对**继承源 item 的元数据**：图片 id、`MediaRef`、`_pdf_positions`、`positions`、`Sheet`/`SheetIndex`。

**例 8：PDF 里的问答文本（无表格标记）**

上游 JSON item：`{"text": "Q\tA", "image": "img-42", "_pdf_positions": [[1,2,3,4,5]]}`（doc_type 标为 table 但无行可读）→ 按文本路径拆出 `Q`/`A`，切片同时携带 `image: img-42` 和坐标（见 [qa_batch2_test.go:111](internal/ingestion/component/chunker/qa_batch2_test.go#L111)）。

**例 9：电子表格合并单元格**

`ResolveRows(td, RowsQA)`（[table_rows.go:45](internal/parser/tableutil/table_rows.go#L45)）定义了 QA 视角的合并单元格投影：

- **行合并（rowspan）**：值在"原点列"向下投影到被覆盖的行 —— QA 把**每一行都当数据行**，所以表头行发起的 rowspan 也会填入数据行；
- **列合并（colspan）**：**不向右复制** —— 一个跨两列的问题必须和下一个非空格配对，而不是和自己配对（否则问题 = 答案 = 同一文本）。

```
┌────────────┬──────────┐
│ 退款政策    │ 7 天     │   ← "退款政策" 若跨两列合并，第二格留空
│ (colspan=2)│          │
├────────────┼──────────┤
│ 换货政策    │ 30 天    │
└────────────┴──────────┘
```
第一行配对结果：问题=`退款政策`，答案=`7 天`（跳过被合并覆盖的空格）。

**例 10：电子表格按行坐标（row-aligned positions）**

xlsx 等电子表格的 item 带有"每 `<tr>` 一个五元组"的 positions 矩阵。当矩阵长度与行数一致且每元组 5 个字段时（[qa.go:496-504](internal/ingestion/component/chunker/qa.go#L496-L504)）：

- 每个问答对只携带**自己那一行**的五元组（而不是整表矩阵），前端高亮回显能精确定位到行；
- `top_int = positions[i][1] - 1`（rowStart 转 0 基行号），复刻旧 row-IR 编号。

若 positions 不是这种矩阵形态（例如 PDF 的版面坐标盒写进了同名字段），则**不**做此解读，问答对按提取顺序编号——两种语义共用一个字段但绝不混淆。

---

## 4. 问答对 → 切片（ChunkDoc）的组装

提取出 `[]qaPair` 后，[qa.go:132-169](internal/ingestion/component/chunker/qa.go#L132-L169) 逐对组装切片：

### 4.1 前缀语言

```go
qPrefix, aPrefix := "问题：", "回答："          // 默认中文
if strings.EqualFold(c.param.Lang, "english") {
    qPrefix, aPrefix = "Question: ", "Answer: "
}
```
**默认中文**，只有显式 `lang=english` 才切换英文（对齐旧 Python 的默认行为）。

### 4.2 历史前缀清洗（rmQAPrefix）

[rmQAPrefixRe](internal/ingestion/component/chunker/qa.go#L199) 会剥掉问题/答案自带的前缀标记：

```
^(问题|答案|回答|user|assistant|Q|A|Question|Answer|问|答)[\t:：： ]+
```

分隔符是 `[\t:： ]+`（一个或多个），所以 `Q:: answer` 这类多重分隔也会被整段剥掉。

**例 11**：输入 `Question: What is Go?\tAnswer: Go is a language.` → 两条前缀都剥掉，再由组件重新加上规范前缀，最终切片仍是 `Question: What is Go?⇥Answer: Go is a language.`（不会出现双重前缀，见 [qa_test.go](internal/ingestion/component/chunker/qa_test.go) 的 `TestQAChunker_RmQAPrefix`）。

### 4.3 切片文本载体与索引字段

每个切片的最终形状：

| 字段 | 内容 | 说明 |
|---|---|---|
| `text` | `"{qPrefix}{问题}{tab}{aPrefix}{答案}"` | **规范载体**。ingestion 在索引边界把它哈希成 chunk id，并改名为 `content_with_weight`（[process.go:29-34](internal/ingestion/task/indexdoc/process.go#L29-L34)）。若只写 content_with_weight 不写 text，所有空文本切片会共享同一个 id，索引写入时全部问答对坍缩成一个切片——这是必须用 text 作载体的原因 |
| `content_ltks` | 对**问题**分词 | `tokenizer.New(lang)` 粗粒度分词，供全文检索 |
| `content_sm_ltks` | 对问题细粒度分词 | `FineGrainedTokenize`，供混合检索加权 |
| `doc_type` | `"text"` / `"image"` | 携带图片或 MediaRef 时升级为 `image` |
| `top_int` | `[行号]` | 0 基源行/记录号，来自 `qaPair.RowNum`，前端按行定位用 |
| `positions` | 行级坐标 | 表格行对齐时仅本行的五元组 |
| `_pdf_positions` | PDF 版面坐标 | 从源 item 继承 |
| `image` / `media_ref` | 图片 id / 媒体引用 | 从源 item 继承 |
| `sheet` / `sheet_index` | 工作表名/序号 | 电子表格来源时保留 |

注意：**分词只作用于问题**，答案直接拼入切片文本——问答检索时用问题匹配查询意图，答案靠向量与全文召回覆盖。

---

## 5. 下游边界：索引写入

Tokenizer(Indexer) 节点（`search_method: ["embedding","full_text"]`，`filename_embd_weight: 0.1`）接手后：

1. 对切片做 embedding，写入向量字段；
2. 到达 [indexdoc/process.go:29](internal/ingestion/task/indexdoc/process.go#L29) 的索引边界时执行字段改名：`text` → `content_with_weight` 并删除 `text`，text 值在此边界是权威来源。

---

## 6. 行为契约速查（含出处）

| 契约 | 行为 | 验证测试 |
|---|---|---|
| CSV 首行即数据 | 不跳过表头（`q1,a1` 直接成为第一条问答） | `TestCSVQAFirstRowIsData` |
| CSV 坏行并入上一条答案 | 3 列行折叠进前一条的 Answer | `TestCSVTypedRowsFoldMalformedRowIntoPreviousAnswer` |
| tab 优先级 | `tab >= comma` 即选 tab | `detectDelimiter` |
| 跨行引号字段 | `csv.Reader` 全文解析 | `extractQATextCSV` |
| Markdown 标题栈 | 弹出层级 ≥ 当前的旧标题，栈内标题 `\n` 拼接为问题 | `TestQAChunker_Markdown` |
| 代码块保护 | ``` 围栏内 `#` 不当标题 | `extractQAMarkdown` |
| rowspan 向下投影 / colspan 不复制 | QA 视角合并单元格规则 | [table_rows.go:34-43](internal/parser/tableutil/table_rows.go#L34-L43) |
| 切片 id 唯一性 | text 载体保证每对独立 id | `TestQAChunker_ChunksGetDistinctIndexIDs` |
| top_int 行号 | 每片携带 0 基行号 | `TestQAChunker_SetsTopInt` |
| 图片/坐标继承 | PDF item 的 image、_pdf_positions 保留 | `TestQAChunker_CarriesImageAndPositions` |
| 默认中文前缀 | 无 lang 参数即 `问题：/回答:` | `TestQAChunker_DefaultLangIsChinese` |

运行测试：

```bash
bash build.sh --test ./internal/ingestion/component/chunker/...
```

---

## 7. 已知注意点

1. **升级兼容**：行-IR 之前的旧 payload 不含表格标记，会落到文本提取路径。旧文档需要**重新解析**（re-parse），仅重新切片（re-chunk）不够（[qa.go:431-432](internal/ingestion/component/chunker/qa.go#L431-L432) 注释）。
2. **PDF 表格纯文本载荷**：旧 Python 会整表丢弃 PDF 表格的纯文本载荷，Go 的行遍历器会保留其中可读的问答对——这是有意的增强，不是回归。
3. **`doc_type` 不可信**：任何基于类型标签的路由判断都被"能否读出行"的实证判断取代；排查丢数据问题时先看 `TableData` 与行遍历结果，不要看标签。
