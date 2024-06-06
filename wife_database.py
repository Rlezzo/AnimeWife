import os
import aiosqlite

class WifeDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.pool = None

    async def create_pool(self):
        """初始化数据库连接池"""
        if self.pool is None:
            self.pool = await aiosqlite.connect(self.db_file)

    async def close_pool(self):
        """关闭数据库连接池"""
        if self.pool:
            await self.pool.close()

    async def create_tables(self):
        """创建数据库表"""
        async with self.pool.execute('''CREATE TABLE IF NOT EXISTS DailyDraws (
                            ID INTEGER PRIMARY KEY AUTOINCREMENT,
                            GroupID INTEGER NOT NULL,
                            UserID INTEGER NOT NULL,
                            CharacterName TEXT,
                            AcquisitionMethod TEXT,
                            DrawDate DATE NOT NULL,
                            DrawTime TIME NOT NULL
                        );'''):
            await self.pool.commit()
            
        async with self.pool.execute('''CREATE TABLE IF NOT EXISTS Events (
                            ID INTEGER PRIMARY KEY AUTOINCREMENT,
                            GroupID INTEGER NOT NULL,
                            ActionInitiatorUserID INTEGER NOT NULL,
                            ActionReceiverUserID INTEGER NOT NULL,
                            InitiatorCurrentCharacter TEXT,
                            ReceiverCurrentCharacter TEXT,
                            ActionType TEXT NOT NULL,
                            Result TEXT,
                            EventDate DATE NOT NULL,
                            EventTime TIME NOT NULL
                        );'''):
            await self.pool.commit()
            
        async with self.pool.execute('''CREATE TABLE IF NOT EXISTS CharacterFiles (
                            ID INTEGER PRIMARY KEY AUTOINCREMENT,
                            FileName TEXT NOT NULL UNIQUE,
                            BaseName TEXT NOT NULL UNIQUE
                        );'''):
            await self.pool.commit()

    async def insert_data(self, table, data):
        """通用的插入数据方法"""
        columns = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        async with self.pool.execute(sql, tuple(data.values())):
            await self.pool.commit()

    async def insert_daily_draw(self, group_id, user_id, character_name, acquisition_method, draw_date, draw_time):
        """向DailyDraws表插入记录"""
        await self.insert_data('DailyDraws', {
            'GroupID': group_id,
            'UserID': user_id,
            'CharacterName': character_name,
            'AcquisitionMethod': acquisition_method,
            'DrawDate': draw_date,
            'DrawTime': draw_time
        })

    async def insert_event(self, group_id, initiator_id, receiver_id, initiator_character, receiver_character, action_type, result, event_date, event_time):
        """向Events表插入记录"""
        await self.insert_data('Events', {
            'GroupID': group_id,
            'ActionInitiatorUserID': initiator_id,
            'ActionReceiverUserID': receiver_id,
            'InitiatorCurrentCharacter': initiator_character,
            'ReceiverCurrentCharacter': receiver_character,
            'ActionType': action_type,
            'Result': result,
            'EventDate': event_date,
            'EventTime': event_time
        })

    async def insert_character_file(self, file_name):
        """向CharacterFiles表插入单一图片文件名数据"""
        base_name = os.path.splitext(file_name)[0]
        await self.insert_data('CharacterFiles', {
            'FileName': file_name,
            'BaseName': base_name
        })

    async def generic_select_query(self, table_name, select_columns, conditions=None, group_by=None, order_by=None, limit=None):
        """
        通用的SELECT查询方法
        :param table_name: 表名
        :param select_columns: 要查询的列名，可以是字符串或逗号分隔的列名列表
        :param conditions: 查询条件，字典格式，键为列名，值为条件值（可以包含LIKE条件）
        :param group_by: GROUP BY 子句，字符串格式
        :param order_by: ORDER BY 子句，字符串格式
        :param limit: LIMIT 子句，整数格式
        :return: 查询结果列表，每个元素为字典，键为列名，值为列值
        """
        query = f"SELECT {select_columns} FROM {table_name}"
        params = []

        # 构建WHERE子句
        if conditions:
            condition_clauses = []
            for column, value in conditions.items():
                if isinstance(value, str) and '%' in value:
                    condition_clauses.append(f"{column} LIKE ?")
                else:
                    condition_clauses.append(f"{column} = ?")
                params.append(value)
            query += " WHERE " + " AND ".join(condition_clauses)

        # 构建GROUP BY子句
        if group_by:
            query += f" GROUP BY {group_by}"

        # 构建ORDER BY子句
        if order_by:
            query += f" ORDER BY {order_by}"

        # 构建LIMIT子句
        if limit:
            query += f" LIMIT {limit}"

        async with self.pool.execute(query, params) as cursor:
            columns = [column[0] for column in cursor.description]
            rows = await cursor.fetchall()

        # 将查询结果转换为字典列表
        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            result.append(row_dict)

        return result