# 一个积分管理系统的后端API

**前言**

	关于通讯方式:  允许的数据请求格式: 表单和JSON, 后端返回的数据格式: JSON.
	关于编码方式:  UTF-8

## 1 目录结构
+ **InterfaceModules**
    + activity.py
    + dd.py
    + order.py
    + shoppingCart.py
    + upload.py
+ **src**

		 src文件夹下多是一些代码的源码文件
		 其中.pyx基于Cython
		 .py是纯Python文件,已经停止使用了
	+ verify.pyx
	> verify提供了token生成器和验证方法
    + tool.pyx
	> tool包含了众多的工具函数,比如时间间隔计算,数据格式转换,json编码器等
    + Interface.pyx
	> Interface是一个接口实现类,包含了接口的实际运行逻辑
    + dispatcher_switch.pyx
	> dispatcher_switch用于对包含了一个进行数据类型检验的方法和接口分发方法
    + baseInterface.pyx
	> baseInterface是接口基类,包含了一些接口运行过程中的常用业务逻辑
    + verify.py
    + Interface.py
    + dispatcher_switch.py
    + dispatcher.py
    + baseInterface.py
+ **static**
    + **images**
	> images文件夹包含了一些可能需要返回的常用图片
    + **uploadImgs**
	> uploadImgs文件夹包含了通过接口上传的图片
    + **download**
	> download文件夹包含了用户通过下载接口生成的文件
+ **tool**
	+ tool.pyd
	> tool是tool.pyx编译后生成的文件
+ **config**
	+ dbconfig.py
	> dbconfig存储了数据库的配置信息
	+ config.py
	> config存储了一些文件路径
+ app.py
> Flask app
+ setup.py
> 简单的编译脚本
+ pre_check.py
> pre_check存储了各个接口的规范
+ gunicorn_config.py
> gunicorn配置

## 2 环境/依赖
	python          3.7
	cryptography    3.0
	cx-Oracle       7.2.3
	Cython          0.29.21
	DBUtils         1.3
	dingtalk        0.0.5
	dingtalk-sdk    1.3.7
	Flask           1.1.2
	Flask-Cors      3.0.8
	gevent          20.6.2
	greenlet        0.4.16
	gunicorn        20.0.4
	numpy           1.18.5
	pandas          1.0.5
	pymssql         2.1.4
	requests        2.24.0
	setuptools      49.2.0.post20200714
	simplejson      3.17.2
	urllib3         1.25.9
	xlrd            1.2.0

## 3 使用
首先进入当前工程目录, 执行命令：`cd YOUR PROJECT DIR || exit `  
如果你是通过虚拟环境启动,记得进入虚拟环境, 比如conda: ` conda activate YOUR ENV`  
编译一下文件:`python setup.py build_ext --inplace`  
启动gunicorn: `/YOUR PYTHON DIR/gunicorn -c /YOUR PROJECT DIR/gunicorn_config.py app:app`  

