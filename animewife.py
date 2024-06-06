import hoshino, random, os, re, filetype, datetime, json, aiohttp
from hoshino import Service, R, priv, aiorequests
from hoshino.config import RES_DIR
from hoshino.typing import CQEvent
from hoshino.util import DailyNumberLimiter, FreqLimiter
import asyncio
from html import unescape
from .wife_database import WifeDatabase
from .wife_queries import *
    
#————————————————————基本参数+服务————————————————————#

# 图片路径
imgpath = os.path.join(os.path.expanduser(RES_DIR), 'img', 'wife')
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

# 命令频率限制,5秒
_flmt = FreqLimiter(5)

sv_help = '''
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
'''.strip()

sv = Service(
    name = '抽老婆',  #功能名
    use_priv = priv.NORMAL, #使用权限   
    manage_priv = priv.ADMIN, #管理权限
    visible = True, #可见性
    enable_on_default = True, #默认启用
    bundle = '娱乐', #分组归类
    help_ = sv_help #帮助说明
    )
    
#————————————————————数据库初始化————————————————————#

# 指定数据库文件路径
db_file_path = os.path.join(os.path.dirname(__file__), 'config', 'db_wife.db')

# 创建WifeDatabase实例
db_manager = WifeDatabase(db_file_path)
# 在插件启动时初始化和创建所需的表
@hoshino.get_bot().server_app.before_serving
async def initialize_database():
    await db_manager.create_pool()
    await db_manager.create_tables()
    # 如果还没载入老婆名-老婆文件名数据
    if await get_character_files_count(db_manager) == 0:
        file_names = os.listdir(imgpath)
        for file_name in file_names:
            await db_manager.insert_character_file(file_name)
            
@hoshino.get_bot().server_app.after_serving
async def close_database():
    await db_manager.close_pool()

"""
DailyDraws表：
    group_id, user_id, target_id, wife_file_name, acquisition:"抽取/寝取/被牛/交换", today, now_time
    注：被牛的条目老婆名都是None
Events表：
    group_id, user_id, target_id, user_wife_name, target_wife_name, action:"查老婆", "self/other", today, now_time
    group_id, user_id, target_id, user_wife_name, target_wife_name, action:"牛老婆", "成功/失败", today, now_time
    group_id, user_id, target_id, user_wife_name, target_wife_name, action:"交换老婆", "同意/拒绝/超时", today, now_time
CharacterFiles表：
    图片文件名，去掉扩展名的老婆名
"""
#————————————————————切换NTR开关初始化————————————————————#
# 文件路径
ntr_status_file = os.path.join(os.path.dirname(__file__), 'config', 'ntr_status.json')
# 用来存储所有群组的NTR状态
ntr_statuses = {}
# 载入NTR状态
def load_ntr_statuses():
    global ntr_statuses
    # 检查文件是否存在
    if not os.path.exists(ntr_status_file):
        # 文件不存在，则创建空的状态文件
        with open(ntr_status_file, 'w', encoding='utf-8') as f:
            json.dump({}, f, ensure_ascii=False, indent=4)
        ntr_statuses = {}
    else:
        # 文件存在，读取内容到ntr_statuses
        with open(ntr_status_file, 'r', encoding='utf-8') as f:
            ntr_statuses = json.load(f)
# 在程序启动时调用
load_ntr_statuses()
def save_ntr_statuses():
    with open(ntr_status_file, 'w', encoding='utf-8') as f:
        json.dump(ntr_statuses, f, ensure_ascii=False, indent=4)

#————————————————————功能：切换NTR开关————————————————————#

@sv.on_fullmatch(("切换NTR开关状态", "切换ntr开关状态"))
async def switch_ntr(bot, ev: CQEvent):
    # 判断权限，只有用户为群管理员或为bot设置的超级管理员才能使用
    u_priv = priv.get_user_priv(ev)
    if u_priv < sv.manage_priv:
        return
    group_id = str(ev.group_id)
    # 取反群的NTR状态
    ntr_statuses[group_id] = not ntr_statuses.get(group_id, False)
    # 保存到文件
    save_ntr_statuses()
    load_ntr_statuses()
    # 提示信息
    await bot.send(ev, 'NTR功能已' + ('开启' if ntr_statuses[group_id] else '关闭'), at_sender=True)

#————————————————————功能：抽老婆————————————————————#

@sv.on_fullmatch('抽老婆')
async def animewife(bot, ev: CQEvent):
    # 获取QQ群、群用户QQ信息
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)
    # 获取今天的日期，转换为字符串格式
    today = str(datetime.date.today())
    # 数据库查询今天、该群、该用户的、最新老婆数据
    wife_name = await get_latest_daily_draw(db_manager, group_id, user_id, today)
    # 如果没有老婆信息，则进行随机选择
    if wife_name is None:
        # 选择一张老婆图片
        wife_name = random.choice(os.listdir(imgpath))
        # 当前时间
        now_time = datetime.datetime.now().strftime("%H:%M:%S")
        # 添加一条新的抽老婆数据
        await db_manager.insert_daily_draw(group_id, user_id, wife_name, "抽取", today, now_time)
    # 生成返回结果
    result = await get_wife_img_result_msg(wife_name, None, None, "self")
    await bot.send(ev,result,at_sender=True)
    
#————————————————————功能：查老婆————————————————————#

@sv.on_prefix('查老婆')
@sv.on_suffix('查老婆')
async def search_wife(bot, ev: CQEvent):
    # 获取QQ群、群用户QQ信息
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)
    target_id = None
    today = str(datetime.date.today())
    now_time = datetime.datetime.now().strftime("%H:%M:%S")
    # 提取目标用户的QQ号
    for seg in ev.message:
        if seg.type == 'at' and seg.data['qq'] != 'all':
            target_id = int(seg.data['qq'])
            break
    # 检查消息内容是否为空
    if (target_id is None) and ev.message.extract_plain_text().strip() == "":
        target_id = user_id
        lookup_type = "self"
    elif target_id is None:
        return
    else:
        lookup_type = "other"
    # 获取用户和目标用户的配置信息
    user_wife_name = await get_latest_daily_draw(db_manager, group_id, user_id, today)
    target_wife_name = await get_latest_daily_draw(db_manager, group_id, target_id, today)
    await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "查老婆", lookup_type, today, now_time)
    # 生成返回结果
    result = await get_wife_img_result_msg(target_wife_name, group_id, target_id, lookup_type)
    await bot.send(ev,result,at_sender=True)

# 供抽老婆和查老婆使用的获得返回结果text
async def get_wife_img_result_msg(wife_name, group_id, user_id, lookup_type):
    if wife_name is None:
        return "未找到老婆信息"
    if lookup_type == "self":
        nick_name = "你"
    else:
        nick_name = await get_nickname_by_uid(group_id, user_id)
    name, _ = os.path.splitext(wife_name)
    result = f'{nick_name}的二次元老婆是{name}哒~\n'
    try:
        wife_img = R.img(f'wife/{wife_name}').cqcode
        result += str(wife_img)
    except Exception as e:
        hoshino.logger.error(f'读取老婆图片时发生错误: {e}')
    return result

#————————————————————功能：添加老婆————————————————————#

@sv.on_prefix(('添老婆','添加老婆'))
@sv.on_suffix(('添老婆','添加老婆'))
async def add_wife(bot,ev:CQEvent):
    # 获取QQ信息
    user_id = ev.user_id
    group_id = ev.group_id
    # 此注释的代码是仅限bot超级管理员使用，有需可启用并将下面判断权限的代码注释掉
    if user_id not in hoshino.config.SUPERUSERS:
        return
    # 判断权限，只有用户为群管理员或为bot设置的超级管理员才能使用
    # u_priv = priv.get_user_priv(ev)
    # if u_priv < sv.manage_priv:
        # return
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)
    # 检查用户今天是否已添加过老婆信息
    if not mlmt.check(key):
        await bot.send(ev, max_notice, at_sender=True)
        return
    # 提取老婆的名字
    name = ev.message.extract_plain_text().strip()
    # 获得图片信息
    ret = re.search(r"\[CQ:image,file=(.*)?,url=(.*)\]", str(ev.message))
    if not ret:
        # 未获得图片信息
        await bot.send(ev, '请附带二次元老婆图片~')
        return
    # 获取下载url
    url = ret.group(2)
    # 下载图片保存到本地并获取文件名
    try:
        file_name = await download_async(url, name)
    except Exception as e:
        await bot.send(ev, f'下载图片失败: {e}')
        return
    # 插入数据库
    await db_manager.insert_character_file(file_name)
    # 如果不是超级管理员，增加用户的添加老婆次数（管理员可一天增加多次）
    if user_id not in hoshino.config.SUPERUSERS:
        mlmt.increase(key)
    await bot.send(ev, '信息已增加~')
    
#————————————————————交换老婆申请管理器————————————————————#

class ExchangeManager:
    def __init__(self):
        self.active_exchanges = {}

    def add_exchange(self, group_id, user_id, target_id):
        if group_id not in self.active_exchanges:
            self.active_exchanges[group_id] = {}
        self.active_exchanges[group_id][user_id] = target_id

    def remove_exchange(self, group_id, user_id):
        if group_id in self.active_exchanges:
            if user_id in self.active_exchanges[group_id]:
                del self.active_exchanges[group_id][user_id]
            # 如果该群的交换请求集合为空，则删除该群的记录
            if not self.active_exchanges[group_id]:
                del self.active_exchanges[group_id]

    def is_exchange_active(self, group_id, user_id, target_id):
        group_exchanges = self.active_exchanges.get(group_id, {})
        for initiator, target in group_exchanges.items():
            if user_id in (initiator, target) or target_id in (initiator, target):
                return True
        return False
        
    def has_active_exchanges_in_group(self, group_id):
        # 检查特定群是否有交易请求
        return bool(self.active_exchanges.get(group_id, {}))
        
    def get_initiator_if_target(self, group_id, target_id):
        # 检查给定ID是否是任何一个交换请求的被申请者，如果是则返回申请者ID，否则返回None
        group_exchanges = self.active_exchanges.get(group_id, {})
        for initiator, target in group_exchanges.items():
            if target == target_id:
                return initiator
        return None
        
# 创建一个全局的ExchangeManager实例
exchange_manager = ExchangeManager()

#————————————————————功能：交换老婆————————————————————#

@sv.on_prefix('交换老婆')
@sv.on_suffix('交换老婆')
async def exchange_wife(bot, ev: CQEvent):
    if await is_near_midnight():
        await bot.send(ev, '日期即将变更，请第二天再进行交换', at_sender=True)
        return
    # 获取QQ群、群用户QQ信息
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)

    target_id = None
    today = str(datetime.date.today())
    # 提取目标用户的QQ号
    for seg in ev.message:
        if seg.type == 'at' and seg.data['qq'] != 'all':
            target_id = int(seg.data['qq'])
            break
    if not target_id:
        #print("未找到目标用户QQ或者未@对方")
        await bot.send(ev, '请指定一个要交换老婆的目标', at_sender=True)
        return    
    # 检查是否尝试交换给自己
    if user_id == target_id:
        await bot.send(ev, '左手换右手？', at_sender=True)
        return
    # 检查发起者或目标者是否已经在任何交换请求中
    if exchange_manager.is_exchange_active(group_id, user_id, target_id):
        await bot.send(ev, '双方有人正在进行换妻play中，请稍后再试', at_sender=True)
        return
    # 数据库查询今天、该群、双方的、最新老婆数据
    user_wife_name = await get_latest_daily_draw(db_manager, group_id, user_id, today)
    target_wife_name = await get_latest_daily_draw(db_manager, group_id, target_id, today)
    # 检查用户和目标用户是否有老婆信息
    if user_wife_name is None or target_wife_name is None:
        await bot.send(ev, '需要双方都有老婆才能交换', at_sender=True)
        return
    # 添加交易状态，防止交易期间他人再对双方发起交易，或重复发起交易
    exchange_manager.add_exchange(group_id, user_id, target_id)
    # 启动超时计时器
    asyncio.create_task(handle_timeout(group_id, user_id, target_id, user_wife_name, target_wife_name, today))
    # 发送交换请求
    await bot.send(ev, f'[CQ:at,qq={target_id}] 用户 [CQ:at,qq={user_id}] 想要和你交换老婆，是否同意？\n如果同意(拒绝)请在60秒内发送“同意(拒绝)”', at_sender=False)

# 交换老婆回复处理
@sv.on_message('group')
async def ex_wife_reply(bot, ev: CQEvent):
    # 如果该群组内没有交换请求
    if not exchange_manager.has_active_exchanges_in_group(ev.group_id):
        return
    # 存在交换请求
    group_id = ev.group_id
    target_id = ev.user_id
    # 判断该用户是否是被申请者，有就返回申请者id
    user_id = exchange_manager.get_initiator_if_target(group_id, target_id)
    # 不为空则说明有记录
    if user_id:
        # 提取消息文本
        keyword = "".join(seg.data['text'].strip() for seg in ev.message if seg.type == 'text')
        # 寻找关键词的索引位置
        agree_index = keyword.find('同意')
        disagree_index = keyword.find('不同意')
        refuse_index = keyword.find('拒绝')
        # 对“不同意”和“拒绝”做处理，找出两者中首次出现的位置
        disagree_first_index = -1
        if disagree_index != -1 and refuse_index != -1:
            disagree_first_index = min(disagree_index, refuse_index)
        elif disagree_index != -1:
            disagree_first_index = disagree_index
        elif refuse_index != -1:
            disagree_first_index = refuse_index
        # 进行判断
        if disagree_first_index != -1 and (agree_index > disagree_first_index or agree_index == -1):
            # 如果找到“不同意”或“拒绝”，且它们在“同意”之前出现，或者没找到“同意”
            await handle_ex_wife(user_id, target_id, group_id, False)
            await bot.send(ev, '对方拒绝了你的交换请求', at_sender=True)
        elif agree_index != -1 and (disagree_first_index > agree_index or disagree_first_index == -1):
            # 如果仅找到“同意”，或者“同意”在“不同意”或“拒绝”之前出现
            await handle_ex_wife(user_id, target_id, group_id, True)
            await bot.send(ev, '交换成功', at_sender=True)

# 处理交换老婆
async def handle_ex_wife(user_id, target_id, group_id, agree):
    today = str(datetime.date.today())
    now_time = datetime.datetime.now().strftime("%H:%M:%S")
    # 数据库查询今天、该群、双方的、最新老婆数据
    user_wife_name = await get_latest_daily_draw(db_manager, group_id, user_id, today)
    target_wife_name = await get_latest_daily_draw(db_manager, group_id, target_id, today)
    if agree:
        # 记录“交换老婆”成功
        await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "交换老婆", "同意", today, now_time)
        # 添加新的老婆记录，互换图片名字添加
        await db_manager.insert_daily_draw(group_id, user_id, target_wife_name, "交换", today, now_time)
        await db_manager.insert_daily_draw(group_id, target_id, user_wife_name, "交换", today, now_time)
    else:
        await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "交换老婆", "拒绝", today, now_time)
    # 删除exchange_manager中该用户的请求
    exchange_manager.remove_exchange(group_id, user_id)

# 交换老婆超时处理
async def handle_timeout(group_id, user_id, target_id, user_wife_name, target_wife_name, today):
    try:
        await asyncio.sleep(60)
        if exchange_manager.get_initiator_if_target(group_id, target_id):
           exchange_manager.remove_exchange(group_id, user_id)
           now_time = datetime.datetime.now().strftime("%H:%M:%S")
           await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "交换老婆", "超时", today, now_time)
           await hoshino.get_bot().send_group_msg(group_id=group_id, message=f"[CQ:at,qq={user_id}] 你的交换请求已超时，对方无视了你")
    except Exception as e:
        hoshino.logger.error(f'交换老婆超时处理时发生错误: {e}')
        
#————————————————————功能：牛老婆————————————————————#

@sv.on_prefix('牛老婆')
@sv.on_suffix('牛老婆')
async def ntr_wife(bot, ev: CQEvent):
    if await is_near_midnight():
        await bot.send(ev, '日期即将变更，请第二天再牛', at_sender=True)
        return
    # 获取QQ群、群用户QQ信息
    load_ntr_statuses()
    group_id = ev.group_id
    if not ntr_statuses.get(str(group_id), False):
        await bot.send(ev, '牛老婆功能未开启！', at_sender=False)
        return
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)
    if not ntr_lmt.check(key):
        await bot.send(ev, ntr_max_notice, at_sender=True)
        return
    target_id = None
    today = str(datetime.date.today())
    # 提取目标用户的QQ号
    for seg in ev.message:
        if seg.type == 'at' and seg.data['qq'] != 'all':
            target_id = int(seg.data['qq'])
            break
    if not target_id:
        #print("未找到目标用户QQ或者未@对方")
        await bot.send(ev, '请指定一个要下手的目标', at_sender=True)
        return
    # 检查是否尝试交换给自己
    if user_id == target_id:
        await bot.send(ev, '不能牛自己', at_sender=True)
        return
    # 检查发起者或目标者是否已经在任何交换请求中
    if exchange_manager.is_exchange_active(group_id, user_id, target_id):
        await bot.send(ev, '双方有人正在进行换妻play中，请稍后再牛', at_sender=True)
        return
    # 数据库查询今天、该群、双方的、最新老婆数据
    user_wife_name = await get_latest_daily_draw(db_manager, group_id, user_id, today)
    target_wife_name = await get_latest_daily_draw(db_manager, group_id, target_id, today)
    # 检查对方是否有老婆
    if target_wife_name is None:
        await bot.send(ev, '需要对方有老婆才能牛', at_sender=True)
        return
    # 满足牛人条件，添加进交换请求列表中，防止牛人期间他人对双方发起交易，产生bug
    exchange_manager.add_exchange(group_id, user_id, target_id)
    now_time = datetime.datetime.now().strftime("%H:%M:%S")
    # 牛老婆次数减少一次
    ntr_lmt.increase(key)
    if random.random() < ntr_possibility: 
        # 记录一次“牛老婆”动作,成功
        await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "牛老婆", "成功", today, now_time)
        # 目标用户新增一条老婆名为None的信息, 代表变成没老婆的状态了
        await db_manager.insert_daily_draw(group_id, target_id, None, "被牛", today, now_time)
        # user新增一条新的老婆信息
        await db_manager.insert_daily_draw(group_id, user_id, target_wife_name, "寝取",today, now_time)
        await bot.send(ev, '你的阴谋已成功！', at_sender=True)
    else:
        await db_manager.insert_event(group_id, user_id, target_id, user_wife_name, target_wife_name, "牛老婆", "失败", today, now_time)
        await bot.send(ev, f'你的阴谋失败了，黄毛被干掉了！你还有{_ntr_max - ntr_lmt.get_num(key)}条命', at_sender=True)
    # 清除交换请求锁
    exchange_manager.remove_exchange(group_id, user_id)
    
# 重置牛老婆次数限制
@sv.on_prefix('重置牛老婆')
@sv.on_suffix('重置牛老婆')
async def reset_ntr_wife(bot, ev: CQEvent):
    # 获取QQ信息
    user_id = ev.user_id
    group_id = ev.group_id
    # # 此注释的代码是仅限bot超级管理员使用，有需可启用并将下面判断权限的代码注释掉
    # if user_id not in hoshino.config.SUPERUSERS:
        # await bot.send(ev,"该功能仅限bot管理员使用")
        # return
    # 判断权限，只有用户为群管理员或为bot设置的超级管理员才能使用
    u_priv = priv.get_user_priv(ev)
    if u_priv < sv.manage_priv:
        await bot.send(ev,"该功能仅限群管理员或为bot设置的超级管理员使用")
        return
    target_id = None
    # 提取目标用户的QQ号
    for seg in ev.message:
        if seg.type == 'at' and seg.data['qq'] != 'all':
            target_id = int(seg.data['qq'])
            break
    target_id = target_id or user_id
    ntr_lmt.reset(f"{group_id}_{target_id}")
    await bot.send(ev,"已重置次数")
        
#————————————————————功能：角色出场率————————————————————#

async def anime_wife_rate(bot, ev: CQEvent):
    """获得本群和所有群的老婆出场率前五"""
    # 获取QQ群、群用户QQ信息
    group_id = ev.group_id
    # 获取本群的前五名角色及其百分比
    group_top_five_characters = await get_top_characters_by_acquisition(db_manager, "抽取", group_id=group_id)
    
    # 获取所有群的前五名角色及其百分比
    top_five_characters = await get_top_characters_by_acquisition(db_manager, "抽取")
    
    # 格式化本群出场率
    if group_top_five_characters:
        group_result_list = [
            f"{os.path.splitext(character['CharacterName'])[0]} {character['Count']}次 {character['Percentage']:.2f}%"
            for character in group_top_five_characters
        ]
        group_result_msg = "本群老婆出场率前五：\n" + "\n".join(group_result_list)
    else:
        group_result_msg = "本群没有抽签记录。"
    
    # 格式化所有群的出场率
    if top_five_characters:
        total_result_list = [
            f"{os.path.splitext(character['CharacterName'])[0]} {character['Count']}次 {character['Percentage']:.2f}%"
            for character in top_five_characters
        ]
        total_result_msg = "总老婆出场率（所有群）：\n" + "\n".join(total_result_list)
    else:
        total_result_msg = "没有抽签记录。"
    
    # 总老婆数
    character_total_count = await get_character_files_count(db_manager)
    
    # 合并消息并发送
    result_msg = f"现有老婆总数：{character_total_count}\n" + group_result_msg + "\n\n" + total_result_msg
    await bot.send(ev, result_msg)

@sv.on_suffix('出场率')
async def character_rate(bot, ev: CQEvent):
    """
    获得特定角色出场率,输出格式：
    角色名：名字
    [图片]
    本群抽中次数：x
    本群抽取率：x%
    总抽中次数：x
    总抽取率：x%
    谁抽到他的次数最多：NAME_ABC
    """
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)

    # 获取命令中的角色名
    character_name = ev.message.extract_plain_text().strip()
    if not character_name:
        await anime_wife_rate(bot, ev)
        return
        
    # 调用辅助函数解析角色名
    character_name = await resolve_character_name(bot, ev, character_name)
    if not character_name:
        return
            
    # 获得带后缀的文件名和图片
    character_name_with_suffix, wife_img = await get_character_img_filename(character_name)
    
    # 获取角色在本群和所有群被抽到的次数和抽取率
    group_stats = await get_character_acquisition_stats(db_manager, character_name_with_suffix, "抽取", group_id=group_id)
    total_stats = await get_character_acquisition_stats(db_manager, character_name_with_suffix, "抽取")
    
    # 获取抽到该角色次数最多的用户
    max_users = await get_max_users_for_character_by_acquisition(db_manager, character_name_with_suffix, "抽取", group_id=group_id)

    # 获取用户昵称并格式化信息
    member_info = await get_member_info_by_gid(group_id)
    max_users_info_list = []
    for user in max_users:
        nick_name = await get_nickname_by_uid(group_id, user['UserID'], member_info)
        max_users_info_list.append(f"{nick_name}（{user['Count']}次）")
        
    max_users_info = "\n".join(max_users_info_list) if max_users_info_list else "无记录"
    
    result_msg = (f"角色名：{character_name}\n{wife_img}\n"
                  f"本群抽中次数：{group_stats['character_count']}\n"
                  f"本群抽取率：{group_stats['percentage']:.2f}%\n"
                  f"总抽中次数：{total_stats['character_count']}\n"
                  f"总抽取率：{total_stats['percentage']:.2f}%\n"
                  f"谁抽到他的次数最多：\n{max_users_info}")
    
    await bot.send(ev, result_msg)

#————————————————————功能：角色寝取率————————————————————#

async def anime_wife_ntr_rate(bot, ev: CQEvent):
    group_id = ev.group_id
    member_info = await get_member_info_by_gid(group_id)
    
    # 本群被寝取次数最多的角色列表
    group_top_characters_by_action_and_result = await get_top_entities_by_action_and_result(db_manager, "character", "牛老婆", "成功", group_id)
    group_action_result_msg_list = []
    for character in group_top_characters_by_action_and_result:
        name = os.path.splitext(character['CharacterName'])[0]
        total_count = character['TotalCount']
        result_count = character['ResultCount']
        result_rate = character['ResultRate']
        group_action_result_msg_list.append(f"「{name}」:\n被牛：{total_count}次 | 到手次数：{result_count}\n成功率：{result_rate:.2f}%")
    group_action_result_msg = "- 本群被牛最多的「老婆」中最好得手的：\n" + "\n".join(group_action_result_msg_list)

    # 所有群被寝取次数最多的角色列表
    total_top_characters_by_action_and_result = await get_top_entities_by_action_and_result(db_manager, "character", "牛老婆", "成功")
    total_action_result_msg_list = []
    for character in total_top_characters_by_action_and_result:
        name = os.path.splitext(character['CharacterName'])[0]
        total_count = character['TotalCount']
        result_count = character['ResultCount']
        result_rate = character['ResultRate']
        total_action_result_msg_list.append(f"「{name}」：\n被牛：{total_count}次 | 到手次数：{result_count}\n成功率：{result_rate:.2f}%")
    total_action_result_msg = "- 所有群被牛最多的「老婆」中最好得手的：\n" + "\n".join(total_action_result_msg_list)

    # 本群主动寝取最多的用户列表
    top_users_by_action_and_result = await get_top_entities_by_action_and_result(db_manager, "user", "牛老婆", "成功", group_id, "initiator")
    top_action_result_user_msg_list = []
    for user in top_users_by_action_and_result:
        nick_name = await get_nickname_by_uid(group_id, user['UserID'], member_info)
        top_action_result_user_msg_list.append(f"@{nick_name}：\n寝取次数：{user['TotalCount']} | 成功次数：{user['ResultCount']}\n成功率：{user['ResultRate']:.2f}%")
    top_action_result_user_msg = "- 本群牛老婆次数最多的人中成功率最高的：\n" + "\n".join(top_action_result_user_msg_list)

    # 本群被寝取最多的用户列表
    top_receivers_by_action_and_result = await get_top_entities_by_action_and_result(db_manager, "user", "牛老婆", "成功", group_id, "receiver")
    top_receivers_count_user_msg_list = []
    for user in top_receivers_by_action_and_result:
        nick_name = await get_nickname_by_uid(group_id, user['UserID'], member_info)
        top_receivers_count_user_msg_list.append(f"@{nick_name}：被牛次数：{user['TotalCount']} | 被得手次数：{user['ResultCount']}\n被牛得手率：{user['ResultRate']:.2f}%")
    top_receivers_count_user_msg = "- 本群中被牛次数最多的人中最苦主的：\n" + "\n".join(top_receivers_count_user_msg_list)

    # 合并消息并发送
    result_msg = (
        group_action_result_msg + "\n\n" +
        total_action_result_msg + "\n\n" +
        top_action_result_user_msg + "\n\n" +
        top_receivers_count_user_msg
    )
    await bot.send(ev, result_msg)

@sv.on_suffix('寝取率')
async def character_ntr_rate(bot, ev: CQEvent):
    """
    获得特定角色寝取率,输出格式：
    角色名：名字
    [图片]
    本群被牛次数：x
    本群牛到手概率：x%
    本群被牛概率：x 本群被牛次数/本群所有角色被牛次数
    所有群总被牛次数：x
    所有群总牛到手率：x%
    总所有被牛的角色中被牛概率：x 所有群被牛次数/所有群所有角色被牛次数
    本群谁牛他的次数最多：NAME_ABC
    本群谁牛到他的次数最多：NAME_EFG，概率为xx
    本群谁持有他时被牛走的次数最多：NAME_CVG，比例为xx
    """
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)
    # 获取命令中的角色名
    character_name = ev.message.extract_plain_text().strip()
    if not character_name:
        await anime_wife_ntr_rate(bot, ev)
        return
        
    # 调用辅助函数解析角色名
    character_name = await resolve_character_name(bot, ev, character_name)
    if not character_name:
        return
    # 获得带后缀的文件名和图片
    character_name_with_suffix, wife_img = await get_character_img_filename(character_name)
    # 获取群成员信息
    member_info = await get_member_info_by_gid(group_id)
    # 获取角色本群统计数据
    group_stats = await get_action_stats(db_manager, "character", character_name_with_suffix, "牛老婆", "成功", "receiver", group_id)
    # 本群牛老婆总次数
    total_count_group = await get_action_total_count(db_manager, "牛老婆", None, group_id)
    # 获取角色全体统计数据
    total_stats = await get_action_stats(db_manager, "character", character_name_with_suffix, "牛老婆", "成功", "receiver")
    # 牛老婆总次数
    total_count_all = await get_action_total_count(db_manager, "牛老婆")
    # 生成结果消息
    result_msg = (
        f"角色名：{character_name}\n{wife_img}\n"
        "所有群：\n"
        f"- 角色被牛总次数：{total_stats['total_action_count']}\n"
        f"- 角色被牛概率：{total_stats['total_action_count']/total_count_all:.2%}\n"
        f"- 角色被牛到手概率：{total_stats['successful_action_count']/total_stats['total_action_count']:.2%}\n\n"
        "本群：\n"
        f"- 角色被牛总次数：{group_stats['total_action_count']}\n"
        f"- 角色被牛概率：{group_stats['total_action_count']/total_count_group:.2%}\n"
        f"- 角色被牛到手概率：{group_stats['successful_action_count']/group_stats['total_action_count']:.2%}\n\n"
    )

        # 本群谁牛角色的次数最多
    top_initiators = await get_top_users_by_action_and_result(
        db_manager,
        character_name_with_suffix,
        character_role='receiver',
        user_role='initiator',
        action_type='牛老婆',
        group_id=group_id
    )

    # 本群谁牛他且结果成功的次数最多
    top_successful_initiators = await get_top_users_by_action_and_result(
        db_manager,
        character_name_with_suffix,
        character_role='receiver',
        user_role='initiator',
        action_type='牛老婆',
        result='成功',
        group_id=group_id
    )

    # 获取本群谁持有某角色时被牛走的次数最多
    top_holders = await get_top_users_by_action_and_result(
        db_manager,
        character_name_with_suffix,
        character_role='receiver',
        user_role='receiver',
        action_type='牛老婆',
        result='成功',
        group_id=group_id
    )

    result_msg += "- 谁牛的次数最多：\n"
    for initiator in top_initiators:
        nick_name = await get_nickname_by_uid(group_id, initiator['UserID'], member_info)
        result_msg += f"@{nick_name}：{initiator['TotalCount']}次\n"

    result_msg += "\n- 谁牛到的次数最多：\n"
    for initiator in top_successful_initiators:
        nick_name = await get_nickname_by_uid(group_id, initiator['UserID'], member_info)
        result_msg += f"@{nick_name}：{initiator['TotalCount']}次\n"

    result_msg += "\n- 谁持有时被牛走的次数最多：\n"
    for holder in top_holders:
        nick_name = await get_nickname_by_uid(group_id, holder['UserID'], member_info)
        result_msg += f"@{nick_name}：{holder['TotalCount']}次\n"

    await bot.send(ev, result_msg)

#————————————————————功能：用户抽老婆数据————————————————————#

@sv.on_prefix('老婆档案')
@sv.on_suffix('老婆档案')
async def search_wife_archive(bot, ev: CQEvent):
    # 获取QQ群、群用户QQ信息
    group_id = ev.group_id
    user_id = ev.user_id
    # 命令频率限制
    key = f"{group_id}_{user_id}"
    if not _flmt.check(key):
        await bot.send(ev, f'操作太频繁，请在{int(_flmt.left_time(key))}秒后再试')
        return
    _flmt.start_cd(key)

    target_id = None
    # 提取目标用户的QQ号
    for seg in ev.message:
        if seg.type == 'at' and seg.data['qq'] != 'all':
            target_id = int(seg.data['qq'])
            break
    # 检查消息内容是否为空
    if (target_id is None) and ev.message.extract_plain_text().strip() == "":
        target_id = user_id
    elif target_id is None:
        return
    member_info = await get_member_info_by_gid(group_id)
    # 抽到过X个老婆/总老婆数
    drawn_wives_count = await get_user_draw_count(db_manager, target_id, "抽取", group_id, True)
    total_wives_count = await get_character_files_count(db_manager)
    # 抽老婆次数：
    drawn_count = await get_user_draw_count(db_manager, target_id, "抽取", group_id)
    # 抽到最多的老婆是：
    top_drawn_wife = await get_top_characters_by_acquisition(db_manager, "抽取", group_id, target_id, limit=1)
    # 牛老婆次数，成功率：
    ntr_stats = await get_action_stats(db_manager, 'user', target_id, "牛老婆", "成功", "initiator", group_id)
    # 最喜欢牛的老婆是：
    top_ntr_target = await get_top_characters_by_user_and_action(db_manager, target_id, "initiator", "receiver", "牛老婆", None, group_id, limit=1)
    # 牛到手最多的是：
    top_successful_ntr_target = await get_top_characters_by_user_and_action(db_manager, target_id, "initiator", "receiver", "牛老婆", "成功", group_id, limit=1)
    # 最喜欢牛的群友是：
    top_ntr_members = await get_top_users_by_role_and_action(db_manager, target_id, "initiator", "receiver", "牛老婆", None, group_id, limit=1)
    # 成功牛到最多的群友是：
    top_successful_ntr_members = await get_top_users_by_role_and_action(db_manager, target_id, "initiator", "receiver", "牛老婆", "成功", group_id, limit=1)
    # 被牛次数：
    ntr_taken = await get_action_stats(db_manager, 'user', target_id, "牛老婆", "成功", "receiver", group_id)
    # 被牛走最多的老婆是：
    top_ntr_taken_characters = await get_top_characters_by_user_and_action(db_manager, target_id, "receiver", "receiver", "牛老婆", "成功", group_id, limit=1)
    # 被谁（群友）成功牛走最多：
    top_successful_ntr_initiator = await get_top_users_by_role_and_action(db_manager, target_id, "receiver", "initiator", "牛老婆", "成功", group_id, limit=1)
    # 交换老婆次数和他人同意次数
    swap_stats = await get_action_stats(db_manager, 'user', target_id, "交换老婆", "同意", role="initiator", group_id=group_id)
    # 最喜欢交换的老婆是：
    top_swap_target = await get_top_characters_by_user_and_action(db_manager, target_id, "initiator", "receiver", "交换老婆", None, group_id, limit=1)
    # 最喜欢和谁换妻
    top_swap_members = await get_top_users_by_role_and_action(db_manager, target_id, "initiator", "receiver", "交换老婆", "同意", group_id, limit=1)
    # 构建响应消息
    nick_name = await get_nickname_by_uid(group_id, target_id, member_info)
    response_message = (
        f"@{nick_name}的老婆档案：\n"
        f"- 老婆图鉴解锁数量：{drawn_wives_count} / {total_wives_count}\n"
        f"- 抽老婆次数：{drawn_count}\n"
    )

    response_message += "- 抽到最多的老婆是：\n"
    for wife in top_drawn_wife:
        response_message += f"{os.path.splitext(wife['CharacterName'])[0]}（{wife['Count']} 次）\n"

    response_message += f"- 牛老婆次数：{ntr_stats['total_action_count']}, 成功率：{ntr_stats['successful_action_count']/ntr_stats['total_action_count']:.2%}\n"

    response_message += "- 最喜欢牛的老婆是：\n"
    for target in top_ntr_target:
        response_message += f"{os.path.splitext(target['CharacterName'])[0]}（{target['TotalCount']} 次）\n"

    response_message += "- 牛到手最多的是：\n"
    for target in top_successful_ntr_target:
        response_message += f"{os.path.splitext(target['CharacterName'])[0]}（{target['TotalCount']} 次）\n"

    response_message += "- 最喜欢牛的群友是：\n"
    for target in top_ntr_members:
        nick_name = await get_nickname_by_uid(group_id, target['TargetUserID'], member_info)
        response_message += f"@{nick_name}（{target['TotalCount']} 次）\n"

    response_message += "- 成功牛到最多的群友是：\n"
    for target in top_successful_ntr_members:
        nick_name = await get_nickname_by_uid(group_id, target['TargetUserID'], member_info)
        response_message += f"@{nick_name}（{target['TotalCount']} 次）\n"

    response_message += f"- 被牛次数：{ntr_taken['total_action_count']}, 苦主率：{ntr_taken['successful_action_count']/ntr_taken['total_action_count']:.2%}\n"

    response_message += "- 被牛走最多的老婆是：\n"
    for character in top_ntr_taken_characters:
        response_message += f"{os.path.splitext(character['CharacterName'])[0]}（{character['TotalCount']} 次）\n"
    
    response_message += "- 被谁牛走最多：\n"
    for initiator in top_successful_ntr_initiator:
        nick_name = await get_nickname_by_uid(group_id, initiator['TargetUserID'], member_info)
        response_message += f"@{nick_name}（{initiator['TotalCount']} 次）\n"

    response_message += f"- 发起交换次数：{swap_stats['total_action_count']}, ta同意次数：{swap_stats['successful_action_count']}\n"

    response_message += "- 最喜欢交换的老婆是：\n"
    for character in top_swap_target:
        response_message += f"{os.path.splitext(character['CharacterName'])[0]}（{character['TotalCount']} 次）\n"
    
    response_message += "- 最喜欢找谁换妻：\n"
    for target in top_swap_members:
        nick_name = await get_nickname_by_uid(group_id, target['TargetUserID'], member_info)
        response_message += f"@{nick_name}（{target['TotalCount']} 次）\n"
    # 发送响应消息
    await bot.send(ev, response_message)

#————————————————————其他方法————————————————————#

# 下载图片
async def download_async(url: str, name: str) -> str:
    url = unescape(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            # 检查响应状态码
            if resp.status == 404:
                raise ValueError('文件不存在')
            content = await resp.read()  # 读取响应内容
            # 通过MIME类型获取文件扩展名
            try:
                mime = filetype.guess_mime(content)
                if mime is None:
                    raise ValueError('无法识别的文件类型')
                extension = mime.split('/')[1]
            except Exception as e:
                raise ValueError(f'不是有效文件类型: {e}')
            # 生成文件保存路径
            abs_path = os.path.join(imgpath, f'{name}.{extension}')
            # 写入文件，使用with语句确保文件对象在操作完成后关闭
            try:
                with open(abs_path, 'wb') as f:
                    f.write(content)
            except Exception as e:
                raise IOError(f'文件写入失败: {e}')
            return f'{name}.{extension}'  # 返回文件名
 
# 判断是否接近跨日，处理跨日前的交换老婆用
async def is_near_midnight():
    """判断距离跨日是否小于2分钟"""
    # 获得当前时间
    now = datetime.datetime.now()
    # 获取第二天的午夜0点时间
    midnight = datetime.datetime(now.year, now.month, now.day) + datetime.timedelta(days=1)
    # 如果现在是23:58以后，也就是离午夜0点小于2分钟
    return now >= midnight - datetime.timedelta(minutes=2)

async def get_member_info_by_gid(group_id):
    """
    获取指定群中所有成员的信息
    :param group_id: 群ID
    :return: 群成员信息列表
    """
    try:
        member_info = await hoshino.get_bot().get_group_member_list(group_id=group_id)
    except Exception as e:
        hoshino.logger.error(f'获取群成员信息时发生错误: {e}, group_id: {group_id}')
        return []
    return member_info
    
async def get_nickname_by_uid(group_id, user_id, member_info=None):
    """
    通过用户ID获取昵称
    :param group_id: 群ID
    :param user_id: 用户ID
    :param member_info: 群成员信息列表，可选
    :return: 用户昵称
    """
    if member_info:
        for member in member_info:
            if member['user_id'] == user_id:
                return member.get('card', '') or member.get('nickname', '') or str(user_id)
    else:
        try:
            member_info = await hoshino.get_bot().get_group_member_info(group_id=group_id, user_id=user_id)
            return member_info.get('card', '') or member_info.get('nickname', '') or str(user_id)
        except Exception as e:
            hoshino.logger.error(f'获取群成员信息时发生错误: {e}, group_id: {group_id}, user_id: {user_id}')
            return str(user_id)

async def get_character_img_filename(character_name):
    """
    通过角色名获取角色的，图片和文件名
    """
    character_name_with_suffix = await get_file_name_by_base_name(db_manager, character_name)
    try:
        wife_img = R.img(f'wife/{character_name_with_suffix}').cqcode
    except Exception as e:
        hoshino.logger.error(f'读取老婆图片时发生错误: {e}')
        wife_img = ""
    return character_name_with_suffix, wife_img

async def resolve_character_name(bot, ev, character_name):
    """
    通过部分名称查询可能的多个角色名，并尝试精确匹配
    :param bot: 机器人实例
    :param ev: 事件实例
    :param character_name: 输入的角色名
    :return: 精确匹配的角色名或None
    """
    # 通过部分名称查询可能的多个角色名
    possible_names = await search_character_by_partial_name(db_manager, character_name)
    if not possible_names:
        await bot.send(ev, "未找到匹配的老婆")
        return None

    # 检查是否存在精确匹配
    exact_match = None
    for possible_name in possible_names:
        if character_name == possible_name:
            exact_match = possible_name
            break

    if exact_match:
        # 如果存在精确匹配，使用精确匹配的角色名
        return exact_match
    else:
        if len(possible_names) > 1:
            # 如果匹配到多个模糊角色名，向用户询问具体是哪一个
            possible_names_str = "\n".join(possible_names)
            await bot.send(ev, f"您可能要找的是：\n{possible_names_str}")
            return None
        else:
            # 如果只匹配到一个角色名，获取老婆本名
            return possible_names[0]
