# 第三方接口对接/数据转换工具：transmit

## 功能描述：

当项目中需要使用同一套数据对接多家第三方接口的时候, 比如对接保险公司接口, 对接支付公司接口. 以往的情况是针对每家公司的接口文档开发一套代码, 这样会添加很多不必要的工作量. 针对这种情况, 我开发了这个工具. 这个工具可以做到接口之间参数转换,转发. 节省了对接接口时的开发任务.

## 优点

1. 数据转换使用freemarker模板, 无需编写java代码.
2. 使用vert.x框架编写. 效率高, 代码量小.
3. 请求数据入库, 数据有迹可循.
4. 可自己编写插件, 完成其定义签名和自定义freemarker指令.

## 使用说明

#### 打包命令
```bash
git clone https://github.com/hjx601496320/transmit.git
cd transmit/
mvn package
cd target/
//解压
tar -zxvf transmit.tar.gz
//启动
sh bin/start.sh
```

#### 文件目录
```
├── bin                         启动脚本
│   ├── restart.sh
│   ├── start.sh
│   └── stop.sh
├── config                      配置
│   ├── config.json
│   └── logback.xml
├── lib
│   ├── commons-cli-1.4.jar     依赖
......
│   ├── transmit-1.0-SNAPSHOT.jar
│   ├── vertx-web-client-3.8.0.jar
│   └── vertx-web-common-3.8.0.jar
├── log                         日志
│   ├── debug
│   │   └── debug.2019-09-17.log
│   ├── error
│   │   └── error.2019-09-17.log
│   └── info
│       └── info.2019-09-17.log
└── sout.log

```


### 配置说明

#### 配置示例

```json
{

  其他配置
  "config": {
    
    是否缓存模板文件,默认true. 关闭的话每次请求会重新加载配置.
    重新加载配置是指 除了port, cache, ext, db节点之外, 每次请求都会重新读所有配置文件(目的是为了方便开发, 线上请不要开启).
    "cache": true,
    
    系统端口号
    "port": 9090,
    
    引用其他的配置文件的文件路径
    "import": [
    ],
    
    其他组件加载, 执行CLass.forName, 可以加载自己定义的一些插件, 完成类似数据入库, 接口签名之类的功能
    "ext": [
      "com.hebaibai.ctrt.Driver"
    ],
    
    数据库配置, 用于保存接口请求日志
    "db": {
      "host": "127.0.0.1",
      "database": "dbname",
      "port": 3306,
      "username": "root",
      "password": "root"
    }
  },
  
  配置示例
  "config-demo": {
  
    接受请求
    "request": {
    
      接受请求的地址 127.0.0.1:9090/download
      "path": "/download",
      
      请求的方式
      "method": "GET",
      
      请求参数类型: FORM(表单提交),  JSON(json),  QUERY(?key=value&key2=value),  TEXT(文本),  XML(xml),
      "request-type": "QUERY",
      
      返回参数类型
      "response-type": "TEXT"
    },
    
    转发的接口配置
    "api": {
    
      接口请求地址
      "url": "http://127.0.0.1:9003/api/download",
      
      插件编号, 在ext中加载来的
      "extCode": "null",
      
      接口请求地址
      "method": "GET",
      
      请求参数类型
      "request-type": "QUERY",
      
      请求超时设置,默认3000 ms, 单位ms
      "timeout": 1,
      
      返回参数类型
      "response-type": "TEXT",
      
      请求参数转换模板
      "request-ftl": "/home/hjx/work/myProject/transmit/file/download-req.ftl",
      
      响应参数转换模板
      "response-ftl": "/home/hjx/work/myProject/transmit/file/download-res.ftl"
    }

    新增配置类型, 用于直接返回文字信息(可以用快速定义一个页面, 接口)
    "text-page": {
      "doc": "测试页面",
      "request": {
        "path": "/index",
        "method": "GET",
        "request-type": "QUERY",
        相应格式为 html
        "response-type": "HTML"
      },
      返回文字信息
      "text": {      
        响应信息的模板文件
        "response-ftl": "result.ftl"
      }
    }
  }
}
```

#### 日志表sql

```sql
-- auto-generated definition
create table api_log
(
  id          varchar(64) null,
  type_code   varchar(64) null
  comment '类型',
  send_msg    text        null
  comment '请求内容',
  receive     text        null
  comment '接口返回数据',
  end_time    datetime    null
  comment '请求耗时',
  create_time datetime    null
  comment '请求时间',
  status      int         null
  comment '状态1:success, 0:error'
)
  comment '接口请求日志';

```

## 接口转换示例

### 请求参数:

```xml
<Demo>
  <Info>
    <Code>XXX-1</Code>
    <UUID>d83a011a-958d-4310-a51b-0fb3a4228ef5</UUID>
	<Time>2017-11-15 16:57:36</Time>
  </Info>
  <XXX>
    <Order>
      <SerialNo>0</SerialNo>
      <OrderNo>123123123</OrderNo>
	  <OrderCode>asdasdasd</OrderCode>
	  <Result>1</Result>
    </Order>
  </XXX>
</Demo>
```

### 转发接口需要的数据:

#### 格式 JSON(POST)

```json
{
    "header": {
        "code": "${ROOT.Info.Code}",
        "date": "${ROOT.Info.Time}"
    },
    "body": {
        "orderCode": "${ROOT.XXX.Order.OrderCode}"
    }
}
```

#### 格式 FORM(POST)

```
code=${ROOT.Info.Code}
date=${ROOT.Info.Time}
orderCode=${ROOT.XXX.Order.OrderCode}
```

#### 格式 QUERY(GET)    

```
code=${ROOT.Info.Code}&date=${ROOT.Info.Time}&orderCode=${ROOT.XXX.Order.OrderCode}
```

#### 格式 XML(POST)           

```xml
<xml>
    <header>
        <code>${ROOT.Info.Code}</code>
        <date>${ROOT.Info.Time}</date>
    </header>
    <body>
        <orderCode>${ROOT.XXX.Order.OrderCode}</orderCode>
    </body>
</xml>
```

## 返回页面实例

#### 配置示例

```json
{
  "config": {
    "port": 8080,
    "cache": false
  },
  "text-page": {
    "doc": "测试页面",
    "request": {
      设置请求路径
      "path": "/index",
      设置请求参数
      "method": "GET",
      请求参数类型
      "request-type": "QUERY",
      设置响应格式(这里是html, 也可以是 xml, json)
      "response-type": "HTML"
    },
    "text": {
      对应的页面文件
      "response-ftl": "result.ftl"
    }
  }
}
```

##### 页面文件: result.ftl

```html
<html>
<head>
    <title>index</title>
</head>
<body>
    <h1>${ROOT.uuid}</h1>
</body>
</html>
```

#### 对应请求地址

`http://127.0.0.1:8080/index?uuid=12`