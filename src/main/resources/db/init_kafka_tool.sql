-- 补偿记录表
create table kafka_compensation_message
(
    `id`                            bigint unsigned  auto_increment not null comment '主键',
    `record_headers`                varchar(2000)    default ''                null comment '消息header',
    `record_key`                    varchar(200)     default ''                null comment '消息key',
    `record_value`                  mediumtext                                 null comment '消息体',
    `record_partition`              int              default 0                 not null comment '分区',
    `record_offset`                 bigint           default -1                not null comment 'offset',
    `record_timestamp`              bigint           default -1                not null comment 'timestamp',
    `record_topic`                  varchar(200)     default ''                not null comment '来源topic',
    `original_method`               varchar(200)     default ''                not null comment '业务方法uri',
    `is_original_method`            tinyint                                    not null comment '是否补偿源方法（0：false，1：true）',
    `compensation_handler`          varchar(200)     default ''                not null comment '自定义补偿handler',
    `compensation_error_handler`    varchar(200)     default ''                not null comment '自定义补偿失败handler',
    `compensation_cluster`          varchar(20)      default ''                null comment '集群',
    `compensation_status`           varchar(20)      default ''                null comment '补偿状态',
    `compensation_count`            int              default 0                 not null comment '重试次数',
    `compensation_limit_max`        int              default 0                 not null comment '限制次数',
    `compensation_interval`         int              default 0                 not null comment '重试间隔',
    `last_compensation_time`        datetime comment '最近补偿时间',
    `next_compensation_time`        datetime comment '下次补偿时间',
    `create_time`                   datetime         default CURRENT_TIMESTAMP not null comment '创建时间',
    `last_update_time`              datetime         default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '最后更新时间',
    `is_delete`                     tinyint unsigned default 0                  not null comment '是否删除（0：未删除，1：已删除）',
    PRIMARY KEY (`id`),
    key idx_compensation_status_and_next_time (compensation_status, next_compensation_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci  comment '补偿消息表' charset = utf8mb4;

create table kafka_send_error_msg
(
    `id`                bigint unsigned auto_increment not null comment '主键' ,
    `trace_id`          varchar(100) default ''                not null comment '消息序号',
    `topic_name`        varchar(100) default ''                not null comment '目的队列名',
    `template_name`     varchar(64)  default ''                not null comment '连接模板名',
    `record_headers`    varchar(2000)    default ''                null comment '消息header',
    `record_key`        varchar(200) default ''                not null comment 'key',
    `record_value`      mediumtext                             null comment '消息体',
    `send_status`       tinyint(1)   default 0                 not null comment '状态：0 待处理，1 已处理',
    `retries`           int          default 0                 not null comment '重试次数',
    `create_time`       datetime     default CURRENT_TIMESTAMP not null comment '创建时间',
    `last_update_time`  datetime     default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '更新时间',
    `is_delete`         tinyint(1)   default 0                 not null comment '是否删除',
    primary key (id),
    key idx_last_update_time (last_update_time),
    key idx_status (send_status)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci comment 'kafka发送异常消息表' charset = utf8mb4;

