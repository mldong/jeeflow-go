# Go · Web 框架接入（统一门面转发层）

> 目标：**任意 Go Web 框架都能在 10 分钟内接入统一门面（JeeflowFacade）**——
> 门面接入 = **1 个路由 + 3 个注入点**，框架差异只在这 ~20 行转发层代码里。
> 引擎初始化、SPI、id 契约都是框架无关的（见 [SDK 集成](./getting-started.md) 与
> [规范 06 统一门面](../../spec/06-facade)）。

## 1. 门面接入模式（四步总则）

```
框架层                                jeeflow 引擎层
┌──────────────────────────┐         ┌──────────────────────┐
│ POST /wf/{action} 路由     │  body   │ facade.Flow          │
│ ① 登录校验（框架已有）      │ ──────→ │  (action, args)       │
│ ② 权限码动态校验            │  args   │  40 个 action 内置路由  │
│ ③ operator 注入            │         └──────────────────────┘
│ ④ id 精度兜底（Go 专属）    │
└──────────────────────────┘
```

| # | 步骤 | 说明 |
|---|------|------|
| 1 | 路由捕获 action | `POST /wf/{action}`，action 是多段路径（`processDefine/page`） |
| 2 | 登录校验 | 用框架已有的登录中间件（门面不感知登录态） |
| 3 | 权限码校验 | 引擎 SPI 提供映射（默认 `wf:{action.replace('/',':')}`），superAdmin 放行（见 [规范 06 §2.6](../../spec/06-facade)） |
| 4 | operator 注入 | `body["operator"] = 当前登录用户 id`——"我的"语义 action 依赖它过滤 |

> **⚠️ Go 专属：雪花 id 精度两连坑（必踩）**
> 1. `json.Unmarshal` 默认把数字解析为 `float64`，雪花 id（>2^53）直接丢精度
>    （`...4290` → `...4288`）——**请求解析必须 `dec.UseNumber()` + 精确转 int64**
> 2. 引擎出口 `stringifyIDs` 只处理 map/slice，**结构体切片原样返回**——响应兜底
>    统一 marshal→unmarshal（UseNumber）成通用 map 后再把超大整数转字符串
>
> 两个坑的处理函数见下文各框架示例（`parseBody` / `idToString` 通用，可直接复制）。

## 2. GoFrame（参考实现）

```go
func (ctrl *WfController) Flow(ctx context.Context, req *wfApi.FlowReq) (*base.CommonResult, error) {
	r := g.RequestFromCtx(ctx)
	// ① 登录校验（框架 AuthMiddleware 全局拦截，路由 meta noPerm:true 豁免后 handler 内校验）
	if !utility.SuperAdmin(ctx) {                 // ② 权限码动态校验
		user, _ := utility.GetCurrentUser(ctx)
		codes := core.PermissionCodes(req.Action)
		if len(codes) > 0 && !hasAny(user.PermCodes, codes) {
			r.Response.WriteJson(base.Fail(99990406, "无权限")); return nil
		}
	}
	// ③ 解析 body（UseNumber 保大整数精度）+ 注入操作人
	body := parseBody(r.GetBody())                // 见 §5 通用函数
	body["operator"] = utility.GetCurrentUserId(ctx)
	// ④ 门面转发
	result := core.GetFacade().Flow(req.Action, body)
	// ⑤ listByType 转换 + id 精度兜底（结构体 → 通用 map → idToString）
	if req.Action == "processDesign/listByType" && result["code"] == 0 {
		// Map<type, items> → [{type, title, items}]
	}
	if data, err := toGeneric(result["data"]); err == nil {
		result["data"] = idToString(data)          // 见 §5 通用函数
	}
	r.Response.WriteJson(result)
	return
}
```

> 完整参考实现：mldong-goframe 集成仓 `internal/modules/wf/controller/wf_controller.go`。

## 3. Gin

```go
r := gin.Default()
r.POST("/wf/*action", func(c *gin.Context) {
	action := c.Param("action")                    // '/processDefine/page' → trim 前导 /
	action = strings.TrimPrefix(action, "/")
	// ① 登录校验（中间件已注入 user）
	user := c.MustGet("user").(*User)
	// ② 权限码动态校验（superAdmin 万能放行）
	codes := core.PermissionCodes(action)
	if !user.SuperAdmin && !hasAny(user.PermCodes, codes) {
		c.JSON(403, base.Fail(99990406, "无权限")); return
	}
	// ③ 解析 body（UseNumber 保精度）+ 注入操作人
	body := parseBody(mustBytes(c.Request.Body))
	body["operator"] = user.Id
	// ④ 门面转发
	result := core.GetFacade().Flow(action, body)
	// ⑤ listByType 转换（同 GoFrame）
	// ⑥ id 精度兜底
	if data, err := toGeneric(result["data"]); err == nil {
		result["data"] = idToString(data)
	}
	c.JSON(200, result)
})
```

## 4. Echo

```go
e := echo.New()
e.POST("/wf/*", func(c echo.Context) error {
	action := strings.TrimPrefix(c.Param("*"), "/")  // 'processDefine/page'
	// ① 登录校验（中间件已注入 user）
	user := c.Get("user").(*User)
	// ② 权限码动态校验（superAdmin 万能放行）
	codes := core.PermissionCodes(action)
	if !user.SuperAdmin && !hasAny(user.PermCodes, codes) {
		return c.JSON(403, base.Fail(99990406, "无权限"))
	}
	// ③ 解析 body + 注入操作人
	body := parseBody(mustBytes(c.Request().Body))
	body["operator"] = user.Id
	// ④ 门面转发
	result := core.GetFacade().Flow(action, body)
	// ⑤ listByType 转换（同 GoFrame）
	// ⑥ id 精度兜底
	if data, err := toGeneric(result["data"]); err == nil {
		result["data"] = idToString(data)
	}
	return c.JSON(200, result)
})
```

## 5. 通用函数（三框架共用，直接复制）

```go
// parseBody 解析请求 body：UseNumber + 精确转 int64——雪花 id（>2^53）保精度
func parseBody(b []byte) map[string]interface{} {
	body := map[string]interface{}{}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	_ = dec.Decode(&body)
	return convertNumbers(body).(map[string]interface{})
}

func convertNumbers(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, vv := range t { t[k] = convertNumbers(vv) }
		return t
	case []interface{}:
		for i, vv := range t { t[i] = convertNumbers(vv) }
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil { return i }
		if f, err := t.Float64(); err == nil { return f }
		return t.String()
	default:
		return v
	}
}

// idToString 递归把超过 JS 安全整数（2^53-1）的整数转字符串——前端 JSON.parse 不丢位
func idToString(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, vv := range t { t[k] = idToString(vv) }
		return t
	case []interface{}:
		for i, vv := range t { t[i] = idToString(vv) }
		return t
	case int64:
		if t > 9007199254740991 || t < -9007199254740991 { return strconv.FormatInt(t, 10) }
		return t
	case uint64:
		if t > 9007199254740991 { return strconv.FormatUint(t, 10) }
		return t
	case float64:
		if t > 9007199254740991 || t < -9007199254740991 { return strconv.FormatFloat(t, 'f', 0, 64) }
		return t
	default:
		return v
	}
}

// toGeneric 任意结构体 → 通用 JSON 类型（marshal→unmarshal，UseNumber 保大整数精度）
func toGeneric(v interface{}) (interface{}, error) {
	b, err := json.Marshal(v)
	if err != nil { return nil, err }
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out interface{}
	if err := dec.Decode(&out); err != nil { return nil, err }
	return out, nil
}
```

## 6. 差异点对照表

| 要点 | GoFrame | Gin | Echo |
|------|---------|-----|------|
| 多段路径捕获 | 路由 meta + 请求体 action 字段 | `*action`（trim `/`） | `*`（trim `/`） |
| 登录上下文 | 框架 context / 中间件 | `c.MustGet("user")` | `c.Get("user")` |
| 响应 | `r.Response.WriteJson` | `c.JSON` | `c.JSON` |
| 参考实现 | mldong-goframe 集成仓 | — | — |

> 其他框架（Chi/Beego/Iris…）同理：套「1 路由 + 3 注入点」模式 + 两个精度函数即可。
> 引擎初始化（仓储/SPI/用户体系映射）见 [SDK 集成](./getting-started.md) 与 [SPI 实现指南](./spi-guide.md)。
