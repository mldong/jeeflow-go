-- PostgreSQL 建表 SQL（jeeflow 参考实现用）——语义与 mysql.sql 一致
-- ID 无自增：主键由应用层 IDGenerator 生成（跨库一致，spec §7）
-- content 用 TEXT 存流程定义 JSON 文本（参考实现存 JSON 字符串，不依赖 jsonb）
CREATE TABLE IF NOT EXISTS wf_process_define (
  id            BIGINT       NOT NULL,
  name          VARCHAR(64)  NOT NULL,
  display_name  VARCHAR(100) NOT NULL,
  type          VARCHAR(32),
  state         INT,
  content       TEXT,
  version       INT,
  create_time   TIMESTAMP(3),
  create_user   VARCHAR(64),
  update_time   TIMESTAMP(3),
  update_user   VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_process_define_name ON wf_process_define (name);

CREATE TABLE IF NOT EXISTS wf_process_instance (
  id                BIGINT      NOT NULL,
  parent_id         BIGINT,
  process_define_id BIGINT,
  state             INT,
  parent_node_name  VARCHAR(100),
  business_no       VARCHAR(64),
  operator          VARCHAR(64),
  expire_time       TIMESTAMP(3),
  variable          TEXT,
  create_time       TIMESTAMP(3),
  create_user       VARCHAR(64),
  update_time       TIMESTAMP(3),
  update_user       VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_process_instance_pfid ON wf_process_instance (process_define_id);
CREATE INDEX IF NOT EXISTS idx_process_instance_operator ON wf_process_instance (operator);

CREATE TABLE IF NOT EXISTS wf_process_task (
  id                 BIGINT       NOT NULL,
  process_instance_id BIGINT      NOT NULL,
  task_name          VARCHAR(100) NOT NULL,
  display_name       VARCHAR(100) NOT NULL,
  task_type          INT,
  perform_type       INT,
  task_state         INT,
  operator           VARCHAR(64),
  finish_time        TIMESTAMP(3),
  expire_time        TIMESTAMP(3),
  form_key           VARCHAR(100),
  task_parent_id     BIGINT,
  variable           TEXT,
  create_time        TIMESTAMP(3),
  create_user        VARCHAR(64),
  update_time        TIMESTAMP(3),
  update_user        VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_process_task_piid ON wf_process_task (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_process_task_name ON wf_process_task (task_name);
CREATE INDEX IF NOT EXISTS idx_process_task_operator ON wf_process_task (operator);

CREATE TABLE IF NOT EXISTS wf_process_task_actor (
  id              BIGINT      NOT NULL,
  process_task_id BIGINT      NOT NULL,
  actor_id        VARCHAR(64) NOT NULL,
  create_time     TIMESTAMP(3),
  create_user     VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_process_task_actor_ptid ON wf_process_task_actor (process_task_id);
CREATE INDEX IF NOT EXISTS idx_process_task_actor_aid ON wf_process_task_actor (actor_id);

CREATE TABLE IF NOT EXISTS wf_process_cc_instance (
  id                  BIGINT      NOT NULL,
  process_instance_id BIGINT      NOT NULL,
  actor_id            VARCHAR(64) NOT NULL,
  state               INT         DEFAULT 0,
  create_time         TIMESTAMP(3),
  create_user         VARCHAR(64),
  update_time         TIMESTAMP(3),
  update_user         VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_process_cc_instance_piid ON wf_process_cc_instance (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_process_cc_instance_aid ON wf_process_cc_instance (actor_id);
