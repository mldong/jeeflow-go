-- MySQL 建表 SQL（jeeflow 测试/参考实现用）——对齐 mldong 框架 boot2 表结构
-- ID 无自增：主键由应用层 IDGenerator 生成（跨库一致，spec §7）
CREATE TABLE IF NOT EXISTS wf_process_define (
  id          BIGINT       NOT NULL COMMENT '主键',
  name        VARCHAR(64)  NOT NULL COMMENT '唯一编码',
  display_name VARCHAR(100) NOT NULL COMMENT '显示名称',
  type        VARCHAR(32)  NULL COMMENT '流程类型',
  state       INT          NULL COMMENT '流程是否可用(1可用；0不可用)',
  content     BLOB         NULL COMMENT '流程模型定义',
  version     INT          NULL COMMENT '版本',
  create_time DATETIME(3)  NULL COMMENT '创建时间',
  create_user VARCHAR(64)  NULL COMMENT '创建用户',
  update_time DATETIME(3)  NULL COMMENT '更新时间',
  update_user VARCHAR(64)  NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_define_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程定义';

CREATE TABLE IF NOT EXISTS wf_process_instance (
  id               BIGINT      NOT NULL COMMENT '主键',
  parent_id        BIGINT      NULL COMMENT '父流程ID，子流程实例才有值',
  process_define_id BIGINT     NULL COMMENT '流程定义ID',
  state            INT         NULL COMMENT '流程实例状态(10：进行中；20：已完成；45：已驳回)',
  parent_node_name VARCHAR(100) NULL COMMENT '父流程依赖的节点名称',
  business_no      VARCHAR(64) NULL COMMENT '业务编号',
  operator         VARCHAR(64) NULL COMMENT '流程发起人',
  expire_time      DATETIME(3) NULL COMMENT '期望完成时间',
  variable         TEXT        NULL COMMENT '附属变量json存储',
  create_time      DATETIME(3) NULL COMMENT '创建时间',
  create_user      VARCHAR(64) NULL COMMENT '创建用户',
  update_time      DATETIME(3) NULL COMMENT '更新时间',
  update_user      VARCHAR(64) NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_instance_pfid (process_define_id),
  KEY idx_process_instance_operator (operator)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程实例';

CREATE TABLE IF NOT EXISTS wf_process_task (
  id                BIGINT       NOT NULL COMMENT '主键',
  process_instance_id BIGINT     NOT NULL COMMENT '流程实例ID',
  task_name         VARCHAR(100) NOT NULL COMMENT '任务名称编码',
  display_name      VARCHAR(100) NOT NULL COMMENT '任务显示名称',
  task_type         INT          NULL COMMENT '任务类型(0：主办任务；1：协办任务)',
  perform_type      INT          NULL COMMENT '参与类型(0：普通参与；1：会签参与)',
  task_state        INT          NULL COMMENT '任务状态(10：进行中；20：已完成；99：已废弃)',
  operator          VARCHAR(64)  NULL COMMENT '任务处理人',
  finish_time       DATETIME(3)  NULL COMMENT '任务完成时间',
  expire_time       DATETIME(3)  NULL COMMENT '任务期待完成时间',
  form_key          VARCHAR(100) NULL COMMENT '任务处理表单KEY',
  task_parent_id    BIGINT       NULL COMMENT '父任务ID',
  variable          TEXT         NULL COMMENT '附属变量json存储',
  create_time       DATETIME(3)  NULL COMMENT '创建时间',
  create_user       VARCHAR(64)  NULL COMMENT '创建用户',
  update_time       DATETIME(3)  NULL COMMENT '更新时间',
  update_user       VARCHAR(64)  NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_task_piid (process_instance_id),
  KEY idx_process_task_name (task_name),
  KEY idx_process_task_operator (operator)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程任务';

CREATE TABLE IF NOT EXISTS wf_process_task_actor (
  id            BIGINT      NOT NULL COMMENT '主键',
  process_task_id BIGINT    NOT NULL COMMENT '流程任务ID',
  actor_id      VARCHAR(64) NOT NULL COMMENT '参与者ID',
  create_time   DATETIME(3) NULL COMMENT '创建时间',
  create_user   VARCHAR(64) NULL COMMENT '创建用户',
  PRIMARY KEY (id),
  KEY idx_process_task_actor_ptid (process_task_id),
  KEY idx_process_task_actor_aid (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程任务和参与人关系';

CREATE TABLE IF NOT EXISTS wf_process_cc_instance (
  id                 BIGINT      NOT NULL COMMENT '主键',
  process_instance_id BIGINT     NOT NULL COMMENT '流程实例ID',
  actor_id           VARCHAR(64) NOT NULL COMMENT '被抄送人ID',
  state              INT         NULL DEFAULT 0 COMMENT '抄送状态(1:已读；0：未读)',
  create_time        DATETIME(3) NULL COMMENT '创建时间',
  create_user        VARCHAR(64) NULL COMMENT '创建用户',
  update_time        DATETIME(3) NULL COMMENT '更新时间',
  update_user        VARCHAR(64) NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_cc_instance_piid (process_instance_id),
  KEY idx_process_cc_instance_aid (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程抄送实例';

CREATE TABLE IF NOT EXISTS wf_process_design (
  id            BIGINT       NOT NULL COMMENT '主键',
  name          VARCHAR(100) NOT NULL COMMENT '流程编码（唯一）',
  display_name  VARCHAR(200) NOT NULL COMMENT '流程显示名称',
  type          VARCHAR(50)  NULL DEFAULT 'approval' COMMENT '流程类型',
  icon          VARCHAR(200) NULL COMMENT '图标',
  is_deployed   INT          NULL DEFAULT 0 COMMENT '是否已部署(0:否；1:是)',
  remark        TEXT         NULL COMMENT '备注',
  create_time   DATETIME(3)  NULL COMMENT '创建时间',
  create_user   VARCHAR(64)  NULL COMMENT '创建用户',
  update_time   DATETIME(3)  NULL COMMENT '更新时间',
  update_user   VARCHAR(64)  NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_design_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程设计';

CREATE TABLE IF NOT EXISTS wf_process_design_his (
  id                BIGINT      NOT NULL COMMENT '主键',
  process_design_id BIGINT      NOT NULL COMMENT '流程设计ID',
  content           BLOB        NULL COMMENT '流程模型定义',
  create_time       DATETIME(3) NULL COMMENT '创建时间',
  create_user       VARCHAR(64) NULL COMMENT '创建用户',
  PRIMARY KEY (id),
  KEY idx_process_design_his_pdid (process_design_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程设计历史';

CREATE TABLE IF NOT EXISTS wf_process_surrogate (
  id            BIGINT      NOT NULL COMMENT '主键',
  process_name  VARCHAR(100) NULL COMMENT '流程编码(为空=全部流程)',
  operator      VARCHAR(64) NOT NULL COMMENT '授权人',
  surrogate     VARCHAR(64) NOT NULL COMMENT '代理人',
  start_time    DATETIME(3) NULL COMMENT '授权开始时间',
  end_time      DATETIME(3) NULL COMMENT '授权结束时间',
  enabled       INT         NULL DEFAULT 1 COMMENT '是否启用(1:启用；0:停用)',
  create_time   DATETIME(3) NULL COMMENT '创建时间',
  create_user   VARCHAR(64) NULL COMMENT '创建用户',
  update_time   DATETIME(3) NULL COMMENT '更新时间',
  update_user   VARCHAR(64) NULL COMMENT '更新用户',
  PRIMARY KEY (id),
  KEY idx_process_surrogate_op (operator),
  KEY idx_process_surrogate_sur (surrogate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程委托代理';
