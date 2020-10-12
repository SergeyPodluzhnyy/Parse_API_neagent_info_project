from flask import Flask
from parse import getNewData
from database import DB, getLastFilePath
import json

app = Flask(__name__)

@app.route('/api/load_new_data/<page_num>')
def loadNewData(page_num ='1'):
	try:
		page_num = int(page_num)
		getNewData(page_num)
		dataBase()

		return json.dumps({"success": True})
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

@app.route('/api/select_from_db/<field>/<value>')#  - /api/select_from_db/district/ГМР
def selectFromDB(field, value):
	try:
		db = DB()
		js = db.dataToJson("aparts", "{} = '{}'".format(field, value))
		return json.dumps(js, ensure_ascii=False)
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

@app.route('/api/cheap_aparts')
def dataFromCheap():
	try:
		db = DB()
		return json.dumps(db.dataToJson("aparts_cheap"), ensure_ascii=False)
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

@app.route('/api/expensive_district')
def dataFromExp():
	try:
		db = DB()
		return json.dumps(db.dataDistrictToJson(), ensure_ascii=False)
	except Exception as e:
		return json.dumps({"status": "err", "error_text": str(e)})

def dataBase():

	fileURL = getLastFilePath()
	db = DB()
	db.csv2sql(fileURL)
	db.createApartsTable()
	db.createTableNewRows()
	db.createTableUpdateRows()
	db.createTableDeleteRows()
	db.createTableCheapAparts()
	db.createTableExpensiveDistricts()
	db.updateApartsTable()
	db.deleteTmpTables()


if  __name__ == '__main__':
	app.debug = True
	app.run()