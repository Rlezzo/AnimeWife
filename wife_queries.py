from .wife_database import WifeDatabase

#————————————————————CharacterFiles 查询————————————————————#

async def get_character_files_count(db: WifeDatabase):
    """获取CharacterFiles表的总数"""
    result = await db.generic_select_query(
        table_name="CharacterFiles",
        select_columns="COUNT(*) as TotalCount"
    )
    return result[0]["TotalCount"] if result else 0

async def search_character_by_partial_name(db: WifeDatabase, partial_name):
    """通过部分名称查询可能的多个角色名"""
    conditions = {"BaseName": f"%{partial_name}%"}
    result = await db.generic_select_query(
        table_name="CharacterFiles",
        select_columns="BaseName",
        conditions=conditions
    )
    return [row["BaseName"] for row in result] if result else []

async def get_file_name_by_base_name(db: WifeDatabase, base_name):
    """通过去掉后缀的文件名查询带后缀的文件名"""
    conditions = {"BaseName": base_name}
    result = await db.generic_select_query(
        table_name="CharacterFiles",
        select_columns="FileName",
        conditions=conditions,
        limit=1
    )
    return result[0]["FileName"] if result else None
    
#————————————————————DailyDraws 查询————————————————————#

async def get_latest_daily_draw(db: WifeDatabase, group_id, user_id, draw_date):
    """获取指定用户在某个日期的最新获取老婆记录"""
    conditions = {"GroupID": group_id, "UserID": user_id, "DrawDate": draw_date}
    latest_draw = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="CharacterName",
        conditions=conditions,
        order_by="ID DESC",
        limit=1
    )
    return latest_draw[0]["CharacterName"] if latest_draw else None

async def get_user_draw_count(db: WifeDatabase, user_id, acquisition_method, group_id=None, distinct=False):
    """
    获取某个用户在指定群组中通过指定方式（如“抽取”）获得的条目数量，
    并且可以选择是否包含重复的角色名。
    
    :param db: 数据库实例
    :param user_id: 用户ID
    :param acquisition_method: 获取方式
    :param group_id: 群组ID（可选）
    :param distinct: 是否统计不重复的角色名（默认统计所有）
    :return: 符合条件的条目数量
    """
    conditions = {"UserID": user_id, "AcquisitionMethod": acquisition_method}
    if group_id:
        conditions["GroupID"] = group_id

    select_columns = "COUNT(*) as Count"
    if distinct:
        select_columns = "COUNT(DISTINCT CharacterName) as Count"

    draw_count_result = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns=select_columns,
        conditions=conditions
    )
    
    draw_count = draw_count_result[0]["Count"] if draw_count_result else 0
    
    return draw_count

async def get_character_acquisition_stats(db: WifeDatabase, character_name_with_suffix, acquisition_method, group_id=None):
    """
    获取指定角色在特定群组或所有群组中通过特定方式（如“抽取”）获得的次数以及总次数，并计算获得的百分比
    """
    conditions_char = {"CharacterName": character_name_with_suffix, "AcquisitionMethod": acquisition_method}
    conditions_total = {"AcquisitionMethod": acquisition_method}
    if group_id:
        conditions_char["GroupID"] = group_id
        conditions_total["GroupID"] = group_id

    character_count_result = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="COUNT(*) as Count",
        conditions=conditions_char
    )
    
    total_count_result = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="COUNT(*) as Count",
        conditions=conditions_total
    )
    
    character_count = character_count_result[0]["Count"] if character_count_result else 0
    total_count = total_count_result[0]["Count"] if total_count_result else 0
    
    percentage = (character_count / total_count * 100) if total_count > 0 else 0
    
    return {
        "character_count": character_count,
        "total_count": total_count,
        "percentage": percentage
    }

async def get_top_characters_by_acquisition(db: WifeDatabase, acquisition_method, group_id=None, user_id=None, limit=5):
    """
    获取指定GroupID或所有群中指定AcquisitionMethod的前N名角色和次数，以及他们的百分比
    :param db: 数据库实例
    :param acquisition_method: 获取方式
    :param group_id: 群组ID（可选）
    :param user_id: 用户ID（可选）
    :param limit: 返回的记录数限制
    :return: 角色和次数的字典列表
    """
    conditions = {"AcquisitionMethod": acquisition_method}
    if group_id is not None:
        conditions["GroupID"] = group_id
    if user_id is not None:
        conditions["UserID"] = user_id

    total_count_result = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="COUNT(*) as TotalCount",
        conditions=conditions
    )
    total_count = total_count_result[0]['TotalCount'] if total_count_result else 0

    top_characters_result = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="CharacterName, COUNT(*) as Count",
        conditions=conditions,
        group_by="CharacterName",
        order_by="Count DESC",
        limit=limit
    )

    for character in top_characters_result:
        character['Percentage'] = (character['Count'] / total_count * 100) if total_count > 0 else 0
    
    return top_characters_result

async def get_max_users_for_character_by_acquisition(db: WifeDatabase, character_name, acquisition_method, group_id=None):
    """
    根据角色名、获取方法和群号查询条目数量最多的用户ID信息。
    
    :param character_name: 角色名称
    :param acquisition_method: 获取方法
    :param group_id: 群号（可选）
    :return: 包含用户ID和条目数量的字典列表
    """
    conditions = {
        "CharacterName": character_name,
        "AcquisitionMethod": acquisition_method
    }
    if group_id is not None:
        conditions["GroupID"] = group_id

    results = await db.generic_select_query(
        table_name="DailyDraws",
        select_columns="UserID, COUNT(*) as Count",
        conditions=conditions,
        group_by="UserID",
        order_by="Count DESC"
    )

    if not results:
        return []
    
    max_count = results[0]['Count']
    max_users = [result for result in results if result['Count'] == max_count]

    return max_users

#————————————————————Events 查询————————————————————#

async def get_action_total_count(db: WifeDatabase, action_type, result=None, group_id=None):
    """
    获取指定动作类型和结果类型（如果指定）的总次数
    :param db: 数据库实例
    :param action_type: 动作类型
    :param result: 结果类型（可选）
    :param group_id: 群组ID（可选）
    :return: 总次数
    """
    conditions = {"ActionType": action_type}
    if result is not None:
        conditions["Result"] = result
    if group_id is not None:
        conditions["GroupID"] = group_id

    total_count_result = await db.generic_select_query(
        table_name="Events",
        select_columns="COUNT(*) as TotalCount",
        conditions=conditions
    )
    total_count = total_count_result[0]["TotalCount"] if total_count_result else 0

    return total_count

async def get_top_entities_by_action_and_result(db: WifeDatabase, entity_type, action_type, result, group_id=None, user_role=None, limit=3):
    """以用户或者角色为筛选条件，返回action最多的角色/目标次数，再筛选符合result的次数最多的，和result次数/action的次数的比例"""
    if entity_type not in ["user", "character"]:
        raise ValueError("Invalid entity_type. Must be 'user' or 'character'.")

    if entity_type == "user" and user_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid user_role. Must be 'initiator' or 'receiver'.")

    entity_column = "ActionInitiatorUserID" if user_role == "initiator" else "ActionReceiverUserID" if entity_type == "user" else "ReceiverCurrentCharacter"
    entity_field = "UserID" if entity_type == "user" else "CharacterName"

    # 构建查询的列名、条件、分组和排序方式
    select_columns = f"{entity_column} as {entity_field}, COUNT(*) as TotalCount, SUM(CASE WHEN Result = '{result}' THEN 1 ELSE 0 END) as ResultCount"
    conditions = {"ActionType": action_type}
    if group_id is not None:
        conditions["GroupID"] = group_id
    group_by = entity_column
    order_by = "TotalCount DESC, ResultCount DESC"

    # 使用通用查询方法获取数据
    top_entities = await db.generic_select_query(
        table_name="Events",
        select_columns=select_columns,
        conditions=conditions,
        group_by=group_by,
        order_by=order_by,
        limit=limit
    )

    if not top_entities:
        return []

    # 找到TotalCount最多的值
    max_total_count = top_entities[0]["TotalCount"]

    # 只保留TotalCount最多的实体
    top_entities = [entity for entity in top_entities if entity["TotalCount"] == max_total_count]

    # 找到ResultCount最多的值
    max_result_count = max(top_entities, key=lambda x: x["ResultCount"])["ResultCount"]

    # 只保留ResultCount最多的实体
    top_entities = [entity for entity in top_entities if entity["ResultCount"] == max_result_count]

    # 计算成功率并更新结果集
    for entity in top_entities:
        total_count = entity["TotalCount"]
        result_count = entity["ResultCount"]
        entity["ResultRate"] = (result_count / total_count * 100) if total_count > 0 else 0

    return top_entities

async def get_action_stats(db: WifeDatabase, entity_type, entity_id, action_type, result=None, role="initiator", group_id=None):
    """
    查询指定实体（用户或角色）在指定群组中指定动作类型的数量和结果为特定值的数量（可选）
    :param db: 数据库实例
    :param entity_type: 实体类型（'user' 或 'character'）
    :param entity_id: 实体ID（用户ID或角色名）
    :param action_type: 动作类型
    :param result: 结果（可选）
    :param group_id: 群组ID（可选）
    :param role: 角色（'initiator' 或 'receiver'）
    :return: 包含总次数和成功次数的字典
    比如查雷姆，牛老婆，成功，receiver。返回雷姆被牛的次数，雷姆成功被牛的次数
    查USERA，牛老婆，成功，initiator。返回USERA牛别人的次数，成功的次数
    receiver，被牛的次数，别人牛他成功的次数
    """
    if entity_type not in ["user", "character"]:
        raise ValueError("Invalid entity_type. Must be 'user' or 'character'.")

    if role not in ["initiator", "receiver"]:
        raise ValueError("Invalid role. Must be 'initiator' or 'receiver'.")

    entity_column = "ActionInitiatorUserID" if role == "initiator" else "ActionReceiverUserID"
    if entity_type == "character":
        entity_column = "ReceiverCurrentCharacter" if role == "receiver" else "InitiatorCurrentCharacter"
    
    # 查询总的动作数量
    conditions_total = {
        entity_column: entity_id,
        "ActionType": action_type
    }
    if group_id is not None:
        conditions_total["GroupID"] = group_id

    total_action_result = await db.generic_select_query(
        table_name="Events",
        select_columns="COUNT(*) as TotalCount",
        conditions=conditions_total
    )
    total_action_count = total_action_result[0]["TotalCount"] if total_action_result else 0

    action_stats = {
        "total_action_count": total_action_count,
        "successful_action_count": 0
    }

    if result:
        # 查询动作且结果为指定结果的数量
        conditions_success = {
            entity_column: entity_id,
            "ActionType": action_type,
            "Result": result
        }
        if group_id is not None:
            conditions_success["GroupID"] = group_id

        successful_action_result = await db.generic_select_query(
            table_name="Events",
            select_columns="COUNT(*) as SuccessCount",
            conditions=conditions_success
        )
        successful_action_count = successful_action_result[0]["SuccessCount"] if successful_action_result else 0
        action_stats["successful_action_count"] = successful_action_count

    return action_stats

async def get_top_users_by_action_and_result(db: WifeDatabase, character_name, character_role, user_role, action_type, result=None, group_id=None, limit=3):
    """
    通过角色名，指定角色是InitiatorCurrentCharacter还是ReceiverCurrentCharacter，
    返回符合条件的用户ID和次数。
    :param db: WifeDatabase实例
    :param character_name: 角色名
    :param character_role: 角色是发起者还是接收者（'initiator' 或 'receiver'）
    :param user_role: 用户角色（'initiator' 或 'receiver'）
    :param action_type: 动作类型
    :param result: 可选的结果类型
    :param group_id: 可选的群组ID
    :param limit: 返回的记录数限制
    :return: 用户ID和次数的列表
    """
    if character_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid character_role. Must be 'initiator' or 'receiver'.")

    if user_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid user_role. Must be 'initiator' or 'receiver'.")

    # 根据 character_role 决定查询的列
    character_column = "InitiatorCurrentCharacter" if character_role == "initiator" else "ReceiverCurrentCharacter"
    user_column = "ActionInitiatorUserID" if user_role == "initiator" else "ActionReceiverUserID"

    # 构建查询的列名、条件、分组和排序方式
    select_columns = f"{user_column} as UserID, COUNT(*) as TotalCount"
    conditions = {
        character_column: character_name,
        "ActionType": action_type
    }
    if result is not None:
        conditions["Result"] = result
    if group_id is not None:
        conditions["GroupID"] = group_id

    group_by = user_column
    order_by = "TotalCount DESC"

    # 使用通用查询方法获取数据
    top_users = await db.generic_select_query(
        table_name="Events",
        select_columns=select_columns,
        conditions=conditions,
        group_by=group_by,
        order_by=order_by,
        limit=limit
    )

    return top_users

async def get_top_characters_by_user_and_action(db: WifeDatabase, user_id, user_role, character_role, action_type, result=None, group_id=None, limit=3):
    """
    通过用户名，指定角色是InitiatorCurrentCharacter还是ReceiverCurrentCharacter，
    返回符合条件的角色名和次数。
    :param db: WifeDatabase实例
    :param user_id: 用户ID
    :param user_role: 用户角色（'initiator' 或 'receiver'）
    :param character_role: 角色是发起者还是接收者（'initiator' 或 'receiver'）
    :param action_type: 动作类型
    :param result: 可选的结果类型
    :param group_id: 可选的群组ID
    :param limit: 返回的记录数限制
    :return: 角色名和次数的列表
    """
    if user_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid user_role. Must be 'initiator' or 'receiver'.")

    if character_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid character_role. Must be 'initiator' or 'receiver'.")

    # 根据 user_role 和 character_role 决定查询的列
    user_column = "ActionInitiatorUserID" if user_role == "initiator" else "ActionReceiverUserID"
    character_column = "InitiatorCurrentCharacter" if character_role == "initiator" else "ReceiverCurrentCharacter"

    # 构建查询的列名、条件、分组和排序方式
    select_columns = f"{character_column} as CharacterName, COUNT(*) as TotalCount"
    conditions = {
        user_column: user_id,
        "ActionType": action_type
    }
    if result is not None:
        conditions["Result"] = result
    if group_id is not None:
        conditions["GroupID"] = group_id

    group_by = character_column
    order_by = "TotalCount DESC"

    # 使用通用查询方法获取数据
    top_characters = await db.generic_select_query(
        table_name="Events",
        select_columns=select_columns,
        conditions=conditions,
        group_by=group_by,
        order_by=order_by,
        limit=limit
    )

    return top_characters

async def get_top_users_by_role_and_action(db: WifeDatabase, user_id, user_role, target_role, action_type, result=None, group_id=None, limit=3):
    """
    通过用户名，指定用户角色和目标角色，返回符合条件的目标用户ID和次数。
    :param db: WifeDatabase实例
    :param user_id: 用户ID
    :param user_role: 用户角色（'initiator' 或 'receiver'）
    :param target_role: 目标角色（'initiator' 或 'receiver'）
    :param action_type: 动作类型
    :param result: 可选的结果类型
    :param group_id: 可选的群组ID
    :param limit: 返回的记录数限制
    :return: 目标用户ID和次数的列表
    """
    if user_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid user_role. Must be 'initiator' or 'receiver'.")

    if target_role not in ["initiator", "receiver"]:
        raise ValueError("Invalid target_role. Must be 'initiator' or 'receiver'.")

    # 根据 user_role 和 target_role 决定查询的列
    user_column = "ActionInitiatorUserID" if user_role == "initiator" else "ActionReceiverUserID"
    target_column = "ActionInitiatorUserID" if target_role == "initiator" else "ActionReceiverUserID"

    # 构建查询的列名、条件、分组和排序方式
    select_columns = f"{target_column} as TargetUserID, COUNT(*) as TotalCount"
    conditions = {
        user_column: user_id,
        "ActionType": action_type
    }
    if result is not None:
        conditions["Result"] = result
    if group_id is not None:
        conditions["GroupID"] = group_id

    group_by = target_column
    order_by = "TotalCount DESC"

    # 使用通用查询方法获取数据
    top_users = await db.generic_select_query(
        table_name="Events",
        select_columns=select_columns,
        conditions=conditions,
        group_by=group_by,
        order_by=order_by,
        limit=limit
    )

    return top_users