package demo

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mldong/jeeflow-go/model"
)

// demoUsers —— 四端（Java/Go/Python/Node）统一同一套 8 个具名用户，
// 切换后端不再"换人"：user1 永远是张三，leader 永远是李四（组长）。
var demoUsers = []struct{ ID, RealName, PostName string }{
	{"user1", "张三", "工程师"},
	{"userA", "孙倩", "工程师"},
	{"userB", "周明", "工程师"},
	{"userC", "吴婷", "工程师"},
	{"leader", "李四", "组长"},
	{"manager", "王五", "经理"},
	{"director", "赵六", "总监"},
	{"boss", "钱七", "总经理"},
}

func findDemoUser(userID string) (realName, postName string, ok bool) {
	for _, u := range demoUsers {
		if u.ID == userID {
			return u.RealName, u.PostName, true
		}
	}
	return "", "", false
}

// demoUserMap 单用户信息 Map（userSearch/candidatePage 行结构）
func demoUserMap(userID string) map[string]interface{} {
	realName, postName := "用户"+userID, "工程师"
	if rn, pn, ok := findDemoUser(userID); ok {
		realName, postName = rn, pn
	}
	return map[string]interface{}{
		"userId": userID, "realName": realName,
		"deptId": "D01", "deptName": "研发部",
		"postId": "P01", "postName": postName,
	}
}

type demoUserProvider struct{}

func (p *demoUserProvider) GetUser(userID string) (*model.UserInfo, error) {
	realName, postName := "用户"+userID, "工程师"
	if rn, pn, ok := findDemoUser(userID); ok {
		realName, postName = rn, pn
	}
	return &model.UserInfo{
		UserID: userID, RealName: realName,
		DeptID: "D01", DeptName: "研发部",
		PostID: "P01", PostName: postName,
	}, nil
}

// demoUserSearch —— 在 8 个演示用户内分页检索（candidatePage 依赖）。
// m_* 条件值统一按关键字处理：对 userId/realName 做包含匹配（演示语义）。
func demoUserSearch(query map[string]interface{}) ([]map[string]interface{}, int, error) {
	var keywords []string
	for k, v := range query {
		if strings.HasPrefix(k, "m_") {
			if s := strings.ToLower(strings.TrimSpace(fmt.Sprint(v))); s != "" && s != "<nil>" {
				keywords = append(keywords, s)
			}
		}
	}
	var all []map[string]interface{}
	for _, u := range demoUsers {
		hit := true
		for _, kw := range keywords {
			if !strings.Contains(strings.ToLower(u.ID), kw) &&
				!strings.Contains(strings.ToLower(u.RealName), kw) {
				hit = false
				break
			}
		}
		if hit {
			all = append(all, demoUserMap(u.ID))
		}
	}
	pageNum, pageSize := toIntDef(query["pageNum"], 1), toIntDef(query["pageSize"], 10)
	if pageNum < 1 {
		pageNum = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	from := (pageNum - 1) * pageSize
	if from > len(all) {
		from = len(all)
	}
	to := from + pageSize
	if to > len(all) {
		to = len(all)
	}
	return all[from:to], len(all), nil
}

func toIntDef(v interface{}, def int) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return int(i)
		}
	}
	return def
}

// demoOrgProvider —— 组织维度取人（部门领导/分管领导/角色），扁平演示组织结构
type demoOrgProvider struct{}

func (demoOrgProvider) FindDeptLeaders(deptID string) ([]string, error) {
	return []string{"leader"}, nil
}

func (demoOrgProvider) FindDeptMainLeaders(deptID string) ([]string, error) {
	return []string{"manager"}, nil
}

func (demoOrgProvider) FindByRole(roleCode string) ([]string, error) {
	switch roleCode {
	case "leader":
		return []string{"leader"}, nil
	case "manager":
		return []string{"manager"}, nil
	case "director":
		return []string{"director"}, nil
	case "boss":
		return []string{"boss"}, nil
	}
	return []string{}, nil
}
