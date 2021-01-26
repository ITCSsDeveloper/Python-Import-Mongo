# Python MongoDB
# REF : https://www.w3schools.com/python/python_mongodb_getstarted.asp

# pip install pymongo

# Import pymongo
import pymongo

# Connection String
myclient = pymongo.MongoClient('mongodb://root:example@localhost:27017')

# db pointer
mydb = myclient['gcc']

# collection pointer
mycol = mydb['gcc']

# Check Database List
dblist = myclient.list_database_names()
print(dblist)
if "gcc" in dblist:
  print("The database exists.")
  
# Check Collection List
collist = mydb.list_collection_names()
print(collist)
if "gcc" in collist:
  print("The collection exists.")

####################################
# Clear Data For Test Script again
mycol.delete_many({})
###################################


# Insert and Return _id
print('Insert and Return _id')
mydict = { "name": "Peter", "address": "Lowstreet 27" }
x = mycol.insert_one(mydict)
print(x.inserted_id)

# Insert Many ( Insert many with specify Id)
print('Insert Many ( Insert many with specify Id)')
mylist = [
  { "_id": 1, "name": "John", "address": "Highway 37"},
  { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
  { "_id": 3, "name": "Amy", "address": "Apple st 652"},
  { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
  { "_id": 5, "name": "Michael", "address": "Valley 345"},
  { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
  { "_id": 7, "name": "Betty", "address": "Green Grass 1"},
  { "_id": 8, "name": "Richard", "address": "Sky st 331"},
  { "_id": 9, "name": "Susan", "address": "One way 98"},
  { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
  { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
  { "_id": 12, "name": "William", "address": "Central st 954"},
  { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
  { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
]
x = mycol.insert_many(mylist)
print(x.inserted_ids)


# Find One
print('Find One')
x = mycol.find_one()
print(x)

# Find All
print('Find All')
for x in mycol.find():
  print(x)


# Return Only Some Fields
print('Return Only Some Fields')
for x in mycol.find({},{ "_id": 0, "name": 1, "address": 1 }):
  print(x)


# Filter the Result
print('Filter the Result')
myquery = { "address": "Park Lane 38" }
mydoc = mycol.find(myquery)
for x in mydoc:
  print(x)


# Advanced Query
print('Advanced Query')
myquery = { "address": { "$gt": "S" } }
mydoc = mycol.find(myquery)
for x in mydoc:
  print(x)
  
# Filter With Regular Expressions
print('Filter With Regular Expressions')
myquery = { "address": { "$regex": "^S" } }
mydoc = mycol.find(myquery)
for x in mydoc:
  print(x)


# Sort the Result
print('Sort the Result')
mydoc = mycol.find().sort("name")
for x in mydoc:
  print(x)
  
# Sort Descending
print('Sort Descending')
mydoc = mycol.find().sort("name", -1)
for x in mydoc:
  print(x)


# Delete Document
print('Delete Document')
myquery = { "address": "Mountain 21" }
mycol.delete_one(myquery)


# Delete Many Documents
print('Delete Many Documents')
myquery = { "address": {"$regex": "^S"} }
x = mycol.delete_many(myquery)
print(x.deleted_count, " documents deleted.")


# Delete All Documents in a Collection
print('Delete All Documents in a Collection')
x = mycol.delete_many({})
print(x.deleted_count, " documents deleted.")

# Delete Collection
# mycol.drop()


# Update Collection
print('Update Collection')
myquery = { "address": "Valley 345" }
newvalues = { "$set": { "address": "Canyon 123" } }
mycol.update_one(myquery, newvalues)
#print "customers" after the update:
for x in mycol.find():
  print(x)

# Update Many
print('Update Many')
myquery = { "address": { "$regex": "^S" } }
newvalues = { "$set": { "name": "Minnie" } }
x = mycol.update_many(myquery, newvalues)
print(x.modified_count, "documents updated.")


# Limit the Result
print('Limit the Result')
myresult = mycol.find().limit(5)
for x in myresult:
  print(x)