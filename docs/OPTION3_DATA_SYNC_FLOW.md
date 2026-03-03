# Option 3 – Data Sync: Luồng xử lý

Tóm tắt luồng xử lý khi chọn **Option 3 (Sync Data)** trong Logistics DB Merger.

---

## 1. Khởi tạo & Input

- Khởi tạo RollbackLogger context: `data_schema`.
- **Input:** Nhập **Tenant Name** (filter theo 1 tenant) hoặc **Enter** (chạy **ALL** tenants).

---

## 2. Giải quyết Tenant (Source & Target)

### 2.1. Chế độ một tenant (đã nhập tên)

- **Source:** Tìm `TenantId` trên MDC theo `TenancyName` hoặc `Name` → `sourceTenantId`.
- **Target:**
  - Nếu tenant đã tồn tại trên ADC (theo TenancyName/Name) → dùng `targetTenantId` đó.
  - Nếu chưa có → clone bản ghi Tenants từ MDC sang ADC, lấy `targetTenantId` = Id mới.
- Kết quả: một cặp `(sourceTenantId, targetTenantId)`.

### 2.2. Chế độ ALL (Enter để trống)

- Lấy danh sách tất cả tenant từ MDC: `SELECT Id, Name, TenancyName FROM Tenants ORDER BY Id`.
- Với mỗi tenant:
  - Tìm hoặc tạo tenant tương ứng trên ADC (giống 2.1).
  - Thu thập danh sách: `allTenantPairs = [(SourceId, TargetId, DisplayName), ...]`.
- Sau này sẽ **foreach** theo `allTenantPairs` để chạy từng tenant.

---

## 3. Chuẩn bị danh sách bảng

- **Tables only in MDC:** Đọc từ file `output/mdc_only_tables.txt` nếu có; không thì gọi `GetTablesOnlyInMdcAsync` → `tablesOnlyInMdc`.
- **Source tables:** Lấy bảng tồn tại trên MDC, loại trừ theo `TableSkipRules`.
- **Thứ tự bảng:**  
  - Ưu tiên theo `MigrationConfig.TableOrder` (các bảng có trong config, đúng thứ tự).  
  - Các bảng còn lại thêm vào cuối.  
  → `orderedTables`.

---

## 4. Smart User Sync (trước khi migrate bảng)

- Khởi tạo `userMapping: Dictionary<long, long>` (Source User Id → Target User Id).
- **ALL:** `foreach (srcId, tgtId, displayName) in allTenantPairs` → gọi `SyncUsersAsync(..., srcId, tgtId, ...)`.
- **Một tenant:** Gọi `SyncUsersAsync(..., sourceTenantId, targetTenantId, ...)`.
- User sync dùng key `(TenantId, UserName)` để tránh trùng khi nhiều tenant; match/insert Users và điền `userMapping` cho các cột audit.

---

## 5. Chuẩn bị kết nối & FK (khi không dry-run)

- Mở kết nối Source/Target.
- Tạo bảng IdMapping nếu chưa có: `IdMappingInt`, `IdMappingBigInt`, `IdMappingGuid`.
- **Disable toàn bộ FK** trên ADC: `DisableAllFkAsync(targetConnection)`.
- **ALL:** Xóa file tracking tenant đã chạy (nếu có) để chạy mới sạch.
- Tạo `migrationBatch = Guid.NewGuid().ToString("N")`.
- **Danh sách tenant cần chạy:**
  - ALL: `tenantsToRun = allTenantPairs`.
  - Một tenant: `tenantsToRun = [(sourceTenantId, targetTenantId, tenantName)]`.

---

## 6. Vòng lặp theo tenant và theo bảng

Với mỗi `(curSourceId, curTargetId, curDisplayName)` trong `tenantsToRun`:

- Gán `src = curSourceId`, `tgt = curTargetId` (dùng cho mọi lệnh migrate trong tenant này).
- **ALL:** In log `[DataSync] --- Tenant: {curDisplayName} (SourceId -> TargetId) ---`.

Với mỗi **table** trong `orderedTables`:

- Bỏ qua: `sysdiagrams`, `Tenants`, `Users`, bảng tên bắt đầu `__`.
- **Target table name:** Áp dụng ExplicitTableMappings hoặc Fuzzy match (MDC → ADC) nếu có; đồng bộ schema với `SyncTableSchemaAsync`.
- Lấy/thêm cache **PK info** cho `targetTable`: `GetPkColumnInfoAsync` → `pkInfoCache`.

### 6.1. Skip bảng global (single PK, không TenantId)

- Nếu bảng có **PK 1 cột**, **không có cột TenantId**:
  - PK int/bigint/guid: kiểm tra đã có bản ghi trong `IdMapping*` cho `(TableName, ColumnName)` chưa.
  - PK khác: kiểm tra `COUNT(*)` từ bảng target.
  - Nếu **đã có** (đã seed ở tenant trước) → **skip**: log `Skipping global single-PK table '...' (no TenantId, already seeded)` và `continue`.

### 6.2. Nhánh MDC-only (`tablesOnlyInMdc.Contains(targetTable)`)

- **Không xóa** dữ liệu bảng (xóa/reset do Option 8).
- Gọi `MigrateTableAsync(..., sourceTenantId: src, targetTenantId: tgt, ...)` (copy theo filter tenant nếu bảng có TenantId).
- Nếu PK int/bigint/guid: bulk insert vào bảng IdMapping tương ứng `(TableName, ColumnName, OldId, NewId, MigrationBatch, TenantId)` từ dữ liệu vừa insert trên target (có filter TenantId nếu bảng có TenantId).

### 6.3. Nhánh bảng chung (có cả MDC & ADC)

- **pkInfo == null:** `MigrateTableAsync` (direct copy, không IdMapping).
- **PK 1 cột nhưng không phải int/bigint/guid:** `MigrateTableNaturalPkAsync`.
- **PK composite:**  
  `MigrateCompositeKeyTableAsync` (staging → INSERT với JOIN IdMapping, filter TenantId trong JOIN IdMapping).
- **PK 1 cột int/bigint/guid:**  
  `CreateStagingTableAsync` → `InsertTableWithIdMappingAsync` (WHERE theo `src` nếu bảng có TenantId), ghi IdMapping qua MERGE OUTPUT.

Sau mỗi bảng (trong cùng tenant): không enable FK.

---

## 7. Cập nhật FK từ IdMapping

- Sau khi xử lý xong **tất cả bảng** của tenant hiện tại:  
  `UpdateFkFromIdMappingAsync(targetConnection, migrationBatch, tgt)`  
  → cập nhật cột FK trên ADC theo bảng IdMapping (filter MigrationBatch và TenantId).

---

## 8. Enable FK (chỉ khi “hết tenant”)

- Nằm trong khối `finally`:
  - **ALL (allTenantPairs != null):** Sau khi chạy xong toàn bộ tenant → `EnableAllFkAsync(targetConnection)` và log `Re-enabled all foreign keys`.
  - **Một tenant:**
    - Ghi `sourceTenantId` vào file tracking (DataSync completed tenant IDs).
    - So sánh tập tenant đã chạy với tập **tất cả** tenant trên source.
    - **Nếu đã đủ** (mọi tenant source đều đã có trong file) → `EnableAllFkAsync`, log `All tenants completed. Re-enabled all foreign keys`, xóa file tracking.
    - **Nếu chưa đủ** → không enable FK, log số tenant đã chạy vs tổng và nhắc FK sẽ bật khi chạy hết tenant.

---

## 9. Dry-run

- Khi `dryRun == true`: không mở connection thực, không gọi Disable/Enable FK, không insert; chỉ in log dạng “Would migrate …” / “Would create …” cho từng tenant và bảng.

---

## Tóm tắt nhanh

| Bước | Mô tả |
|------|--------|
| 1 | Input: Tenant name hoặc ALL |
| 2 | Resolve/create tenant(s) trên Source & Target |
| 3 | Lấy danh sách bảng (MDC-only, ordered) |
| 4 | Sync Users theo tenant, build userMapping |
| 5 | Disable FK, tạo IdMapping tables, chuẩn bị tenantsToRun |
| 6 | Foreach tenant → foreach table: skip global single-PK nếu đã seed, MDC-only hoặc common (MigrateTable / NaturalPk / Composite / InsertWithIdMapping) |
| 7 | UpdateFkFromIdMappingAsync cho tenant vừa chạy |
| 8 | Enable FK chỉ khi đã chạy hết tất cả tenant (ALL một lần hoặc single-tenant đủ lần) |

---

## Output chi tiết khi insert data (Option 3)

Console in ra từng bước xử lý insert để dễ theo dõi:

### Trước mỗi bảng (theo mode)

- **MDC-only:**  
  `[Insert] Table: {table} -> [dbo].[{targetTable}] | Mode: MDC-only (copy from MDC, no delete) | TenantId: {id hoặc all}`

- **Direct MigrateTable (không PK/IdMapping):**  
  `[Insert] Table: ... | Mode: Direct MigrateTable (no PK/IdMapping) | TenantId: ...`

- **Natural PK:**  
  `[Insert] Table: ... | Mode: Natural PK (insert missing only) | TenantId: ...`

- **Composite PK:**  
  `[Insert] Table: ... | Mode: Composite PK (staging -> INSERT with IdMapping JOIN) | TenantId: ...`

- **Staging + MERGE + IdMapping (single PK int/bigint/guid):**  
  `[Insert] Table: ... | Mode: Staging + MERGE + IdMapping (single PK int/bigint/guid) | TenantId: ...`

### Trong lúc / sau khi insert

- **MigrateTableAsync (streaming):**  
  `[Data] Completed {table} | Rows copied: {N} (streaming BulkCopy)`

- **MigrateTableAsync (buffer path, có transform):**  
  `[Data] Completed {table} (Transformed {N} rows)`

- **Filter theo tenant:**  
  `   -> Filtering by TenantId = {id}`  
  `   -> Table has no TenantId. Migrating ALL rows (Global/System Table).`

- **IdMapping (MDC-only bulk):**  
  `   -> IdMapping (MDC-only, bulk): {N} row(s) -> [dbo].[IdMappingInt/BigInt/Guid]`

- **InsertTableWithIdMappingAsync (bulk MERGE):**  
  `   -> Inserted {N} row(s) into [dbo].[{targetTable}]`  
  `   -> IdMapping (bulk): {N} row(s) -> [dbo].[IdMapping...]`  
  `   -> Dropped [dbo].[{staging}]`

- **InsertTableWithIdMappingAsync (chunked):**  
  `   -> Inserted {N} row(s) into [dbo].[{targetTable}] (chunked MERGE)`  
  `   -> IdMapping (chunked): {N} row(s) -> [dbo].[IdMapping...]`

- **Composite key:**  
  `   -> Composite key: inserted {N} row(s) into [dbo].[{targetTable}] (skipped existing).`

- **Natural PK:**  
  `   -> Natural PK: inserted {N} missing row(s) into [dbo].[{targetTable}] (skipped existing).`

### Kết thúc xử lý mỗi bảng

- `   -> Done: {table}`
