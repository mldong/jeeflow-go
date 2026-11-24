package facade

import (
	"testing"

	"github.com/mldong/jeeflow-go/engine"
	"github.com/mldong/jeeflow-go/memory"
	"github.com/mldong/jeeflow-go/model"
)

type sub257FacadeIDGen struct{ n int64 }

func (g *sub257FacadeIDGen) NextID() int64 { g.n++; return g.n }

type sub257FacadeUserProv struct{}

func (sub257FacadeUserProv) GetUser(userID string) (*model.UserInfo, error) {
	return &model.UserInfo{UserID: userID, RealName: userID}, nil
}

func Test257FacadeMissingDefinitionCodeAndMessage(t *testing.T) {
	repo := memory.New()
	eng := engine.New(repo, sub257FacadeUserProv{}, &sub257FacadeIDGen{}, nil)
	f := New(eng, repo, nil)
	content := []byte(`{
		"name":"parentMissingFacade257","displayName":"parentMissingFacade257","type":"approval",
		"nodes":[
			{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}},
			{"id":"apply","type":"snaker:task","properties":{"assignee":"applicant","taskType":0,"performType":0},"text":{"value":"申请"}},
			{"id":"subMissing257","type":"snaker:subProcess","properties":{"processName":"missing257","version":1,"form":"__wrong__"},"text":{"value":"子流程"}},
			{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}
		],
		"edges":[
			{"id":"e1","sourceNodeId":"start","targetNodeId":"apply","properties":{}},
			{"id":"e2","sourceNodeId":"apply","targetNodeId":"subMissing257","properties":{}},
			{"id":"e3","sourceNodeId":"subMissing257","targetNodeId":"end","properties":{}}
		]
	}`)
	def := &model.ProcessDefine{Name: "parentMissingFacade257", DisplayName: "parentMissingFacade257", Type: "approval", State: 1, Version: 1, Content: content}
	repo.AddDefine(def)

	result := f.Flow("processDefine/startAndExecute", map[string]interface{}{
		"processDefineId": def.ID,
		"operator":        "applicant",
	})
	if result["code"] != 20010002 || result["msg"] != "子流程missing257定义不存在" {
		t.Fatalf("facade missing-definition envelope = %#v", result)
	}
}
