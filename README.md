# Python-Import-Mongo

ตัวอย่างโค้ต python read file + insert to mongoDb

โปรแกรมนี้ออกแบบมาเพื่อ ทำการนำเข้าไฟล์ต้นทาง เข้าสู่ MongoDB
โดยใช้ภาษา python ในการทำงาน

โปรแกรมมีโค้ตตัวอย่าง ที่เขียนไว้อย่างง่ายๆ เพื่อการศึกษาสำหรับผู้ที่สนใจ



อธิบายโลจิคโปรแกรม
- โปรแกรมทำการอ่านไฟล์ .GCC ใน folder data
- โดยการอ่านจะอ่านทีละบรรทัดจนจบ
- ในไฟล์ .GCC จะระบุ HT(Header), DT(Data), FT(Footer)
- โปรแกรมจำอ่านจนครบจำนวน limit เมื่อครบตามจำนวนแล้วจะ Insert to Database
- เมื่อโปรแกรมทำงานเสร็จสิ้น จะทำการสรุปผลการทำงาน และ เขียนลง .log



วิธี Run
- เปิด teminal or cmd
- พิมพ์ 'docker-compose up'   (เพื่อ Start Server MongoDB, MongoExpress)
- พิมพ์ ' python main.py ' Enter


command (mac os) : 
- python3 main.py -file_name=./data/LA00000.GCC -map_file_name=./data/LA00000.MAP -limit=100 -mo_database=GCC -mo_collection=CBS_CA -log_name=GCC_CA.log

command (windows)
- python main.py -file_name=data/LA00000.GCC -map_file_name=data/LA00000.MAP -limit=100 -mo_database=GCC -mo_collection=GCC_LA -log_name=GCC_LA.log

Option Parameters
-(Options) -header=HT -body=DT -footer=FT


ขอบคุณครับ
:D

![alt text](https://github.com/ITCSsDeveloper/Python-Import-Mongo/raw/master/Screen%20Shot%202564-01-27%20at%2000.23.32.png)
