# 抽二次元老婆

一个适用于HoshinoBot的随机二次元老婆插件，用sqlite记录了一下各种抽老婆牛老婆的数据，用GPT4写的，没有经过很多测试，只能说能用。

## 如何安装

1. 在HoshinoBot的插件目录modules下clone本项目

    `git clone -b AnimeWifeDB --single-branch https://github.com/Rlezzo/AnimeWife.git`

2. 下载 `Releases` 中的  [wife.rar](https://github.com/Rinco304/AnimeWife/releases/download/v1.0/wife.rar) 并将其解压到 `/res/img` 目录下，**需要保证图片名不重名**，后缀无所谓

3. 在 `config/__bot__.py`的模块列表里加入 `AnimeWife`

4. 重启HoshinoBot

## 怎么使用

```
[抽老婆] 看看今天的二次元老婆是谁
[添加老婆+人物名称+图片] 群管理员每天可以添加一次人物
※为防止bot被封号和数据污染请勿上传太涩与功能无关的图片※
[交换老婆] @某人 + 交换老婆
[牛老婆] 50%概率牛到别人老婆(10次/日)
[查老婆] 加@某人可以查别人老婆
[重置牛老婆] 加@某人可以重置别人牛的次数
[出场率] 统计本群和所有群老婆出场率前五名
[寝取率] 统计本群和所有群老婆被牛的一些信息
[XX出场率] 查询XX(角色名)的出场率信息
[XX寝取率] 查询XX(角色名)的具体被牛数据
[老婆档案] 加@某人可以查他人的数据
[切换ntr开关状态]
```

## 备注

`config` 文件夹是用于记录群友每日抽的老婆信息，不用于配置插件，插件的配置位于 ` animewife.py ` 文件中

```
# 群管理员每天可添加老婆的次数
_max=1
mlmt= DailyNumberLimiter(_max)
# 当超出次数时的提示
max_notice = f'为防止滥用，管理员一天最多可添加{_max}次，若需添加更多请使用 来杯咖啡 联系维护组'

# 每人每天可牛老婆的次数
_ntr_max=1
ntr_lmt= DailyNumberLimiter(_ntr_max)
# 当超出次数时的提示
ntr_max_notice = f'为防止牛头人泛滥，一天最多可牛{_ntr_max}次（无保底），若需添加更多请使用 来杯咖啡 联系维护组'
# 牛老婆的成功率
ntr_possibility = 0.5

# 命令频率限制, 5秒
_flmt = FreqLimiter(5)
```

若 `Releases` 中下载速度太慢或下载失败，可以尝试使用百度网盘下载

[网盘下载](https://pan.baidu.com/s/1FbRtczF1h1jIov_CXU1qew?pwd=amls)
提取码：amls

## 效果图
老婆档案示例：
@小云雀 来海的老婆档案：
- 老婆图鉴解锁数量：0 / 465
- 抽老婆次数：0
- 抽到最多的老婆是：
- 牛老婆次数：0, 成功率：0.00%
- 最喜欢牛的老婆是：
- 牛到手最多的是：
- 最喜欢牛的群友是：
- 成功牛到最多的群友是：
- 被牛次数：0, 苦主率：0.00%
- 被牛走最多的老婆是：
- 被谁牛走最多：
- 发起交换次数：0, ta同意次数：0
- 最喜欢交换的老婆是：
- 最喜欢找谁换妻：
![效果图](mdimg.jpg) 

## 参考致谢

| [dailywife](https://github.com/SonderXiaoming/dailywife) | [@SonderXiaoming](https://github.com/SonderXiaoming) |

| [whattoeat](https://github.com/A-kirami/whattoeat) | [@A-kirami](https://github.com/A-kirami) |

| [zbpwife](https://github.com/FloatTech/zbpwife) |（绝大部分老婆图片都是出自这里，个人也添加了一些）
