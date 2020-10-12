import os
import re
import glob
import sqlite3
import pandas as pd
import json
import collections

def getLastFilePath():
    f=os.listdir('results')
    dirs=[]
    for i in range(len(f)):
	    if re.search("([0-9]{4}_[0-9]{2}_[0-9]{2}__[0-9]{2}_[0-9]{2}_[0-9]{2})", f[i]):
		    dirs.append(f[i])
    dirs.sort()
    dir = dirs[-1]
    f=os.listdir('results'+'/' + dir +'/'+ 'result_01.csv')
    files = glob.glob(os.getcwd() +'/'+ 'results'+'/' + dir +'/'+ 'result_01.csv'+'/'+'*.csv')
    return files[-1].replace("\\", '/')

class DB:

	def __init__(self):
		self.conn = sqlite3.connect('neagent.db')
		self.cursor = self.conn.cursor()

	def csv2sql(self, filePath):
		df = pd.read_csv(filePath, encoding='utf-8')
		df.to_sql('aparts_00', con=self.conn, if_exists='replace')


	def createApartsTable(self):
		self.cursor.execute('''
			create table if not exists aparts(
				id integer primary key autoincrement,
				key int,
				actual_date datetime,
				district varchar(128),
				title varchar(128),
				housing varchar(128),
				street varchar(128),
				owner varchar(128),
				floor_height varchar(128),
				area int,
				price int,
				price_sqm int,
				start_dttm datetime default current_timestamp,
				end_dttm datetime default (datetime('2999-12-31 23:59:59'))
			);
			''')

		self.cursor.execute('''
			create view if not exists v_aparts as
				select 
					id
					, key
					, actual_date
					, district
					, title
					, housing
					, street
					, owner
					, floor_height
					, area
					, price
					, price_sqm
				from aparts 
				where current_timestamp between start_dttm and end_dttm

			;
			''')


	def createTableNewRows(self):
		self.cursor.execute('''
			drop table if exists aparts_new;
		''')
		# create aparts_new
		self.cursor.execute('''
			create table aparts_new as
		 		select t1.*
		 		from aparts_00 t1
		 		left join v_aparts t2
		 		on t1.key = t2.key
		 		where t2.key is null;
		 	''')

	def createTableUpdateRows(self):
		# create aparts_02
		self.cursor.execute('''
			create table aparts_02 as 
				select 
					t1.*
				from aparts_00 t1
				inner join v_aparts t2
				on t1.key = t2.key
				and (
					   t1.actual_date     <> t2.actual_date 
					or t1.district        <> t2.district   
					or t1.title           <> t2.title
					or t1.housing         <> t2.housing
					or t1.street          <> t2.street
					or t1.owner           <> t2.owner
					or t1.floor_height    <> t2.floor_height
					or t1.area            <> t2.area
					or t1.price           <> t2.price
					or t1.price_sqm       <> t2.price_sqm
				);

			''')

	def createTableDeleteRows(self):
		# create aparts_03
		self.cursor.execute('''
			create table aparts_03 as 
				select
					t1.key
				from v_aparts t1
				left join aparts_00 t2
				on t1.key = t2.key
				where t2.key is null;
		''')

	def createTableCheapAparts(self):
		self.cursor.execute('''
			drop table if exists aparts_cheap;
		''')

		self.cursor.execute('''
			create table if not exists aparts_cheap as
				select
					t1.*
				from aparts_00 t1
				inner join v_aparts t2
				on t1.key = t2.key
				where t1.price < t2.price
			''')

	def createTableExpensiveDistricts(self):
		self.cursor.execute('''
			drop table if exists aparts_expn;
		''')

		self.cursor.execute('''
				create table if not exists aparts_expn as
				select
					distinct t1.district
				from aparts_00 t1
				inner join v_aparts t2
				on t1.key = t2.key
				where t1.price > t2.price
			''')

	def updateApartsTable(self):
		# логическое удаление старых записей
		self.cursor.execute('''
			update aparts
			set end_dttm = current_timestamp
			where key in (select key from aparts_03)
			and end_dttm = datetime('2999-12-31 23:59:59');
		''')
		
		# изменение записей
		self.cursor.execute('''
			update aparts
			set end_dttm = current_timestamp
			where key in (select key from aparts_02)
			and end_dttm = datetime('2999-12-31 23:59:59');
		''')



		self.cursor.execute('''
			insert into aparts (
				key,
				actual_date,
				district,
				title,
				housing,
				street,
				owner,
				floor_height,
				area,
				price,
				price_sqm
			)
			select 
				key,
				actual_date,
				district,
				title,
				housing,
				street,
				owner,
				floor_height,
				area,
				price,
				price_sqm
			from aparts_02;
		''')

		# добавление новых данных
		self.cursor.execute('''
			insert into aparts (
				key,
				actual_date,
				district,
				title,
				housing,
				street,
				owner,
				floor_height,
				area,
				price,
				price_sqm
			)
			select 
				key,
				actual_date,
				district,
				title,
				housing,
				street,
				owner,
				floor_height,
				area,
				price,
				price_sqm
			from aparts_new;
		''')

		self.conn.commit()

	def deleteTmpTables(self):
		self.cursor.execute('''
			drop table if exists aparts_00;
		''')
		self.cursor.execute('''
			drop table if exists aparts_02;
		''')
		self.cursor.execute('''
			drop table if exists aparts_03;
		''')

	def dataDistrictToJson(self):     
		self.cursor.execute(r'''
			select
				district
			from aparts_expn
			''')
		rows = self.cursor.fetchall()

		objects_list = []

		for row in rows:
		    d = collections.OrderedDict()
		    d["district"] = row[0]
		    objects_list.append(d)
		j = json.dumps(objects_list)
		return json.loads(j)

	def dataToJson(self, table, cond=None):     # table - one of aparts, aparts_new, aparts_cheap
	                                            # cond - None or "district = 'ГМР'"
                                                    # example - db.dataToJson("aparts", "district = 'РИП'")
		self.cursor.execute(r'''
			select
				key,
				actual_date,
				district,
				title,
				housing,
				street,
				owner,
				floor_height,
				area,
				price,
				price_sqm
			from {} {}
			'''.format(table, ("where " + cond) if cond else ""))
		rows = self.cursor.fetchall()

		objects_list = []

		for row in rows:
		    d = collections.OrderedDict()
		    d["key"] = row[0]
		    d["actual_date"] = row[1]
		    d["district"] = row[2]
		    d["title"] = row[3]
		    d["housing"] = row[4]
		    d["street"] = row[5]
		    d["owner"] = row[6]
		    d["floor_height"] = row[7]
		    d["area"] = row[8]
		    d["price"] = row[9]
		    d["price_sqm"] = row[10]
		    objects_list.append(d)
		j = json.dumps(objects_list)
		return json.loads(j)