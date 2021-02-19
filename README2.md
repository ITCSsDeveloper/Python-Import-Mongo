
"""
    
    อธิบาย Mapfile  ( ./data/LA00000.MAP )

    ทุกครั้งที่จะเอาข้อมูลเข้า Database เราต้องทำการระบุชื่อฟิลด์ให้กับข้อมูลก่อน
    โดย ให้เข้าไประบุไว้ในไฟล์ .MAP

    ใน mapfile จะมีส่วนที่กำหนดค่าอยู่ 3 ส่วน คือ HEADER, BODY, FOOTER
    
    ยกตัวอย่าง (HEADER, BODY, FOOTER ใช้หลักการกำหนดแบบเดียวกัน) :
    #HEADER     ( คือ ระบุว่าข้อมูลต่อจากนี้จะเป็นส่วนของ Column Header )
    #ENDHEADER  ( คือ ระบุว่าให้หยุดอ่านข้อมูลของ  Header )

"""

""" 

* วิธีเรียกใช้งาน
 - python3 main.py -file_name=./data/LA00000.GCC -map_file_name=./data/LA00000.MAP -limit=100 -header=HT -body=DT -footer=FT
 
* อธิบาย Parameter :
 -file_name={value}        ( ให้ใส่ Part ไฟล์ Source )
 -map_file_name={value}    ( ให้ใส่ Part ไฟล์ Mapping  )
 -limit={value}            ( จำนวนข้อมูลต่อรอบการ Insert เช่น ใส่ 100 จะหมายถึง ให้อ่านข้อมูลครบ 100 rows ก่อนถึงค่อยทำการ Insert ลง Database  )
 -header={value}           ( ระบุชุดข้อมูลแบบ Header เช่น ใส่ HT เมื่อโปรแกรมอ่านเจอขึ้นต้นว่า HT โปรแกรมจะเข้าใจว่าบรรทัดนั้นคือข้อมูล Header  )
 -body={value}             ( ระบุชุดข้อมูลแบบ Body   เช่น ใส่ DT เมื่อโปรแกรมอ่านเจอขึ้นต้นว่า DT โปรแกรมจะเข้าใจว่าบรรทัดนั้นคือข้อมูล Body  )
 -footer={value}           ( ระบุชุดข้อมูลแบบ Footer เช่น ใส่ FT เมื่อโปรแกรมอ่านเจอขึ้นต้นว่า FT โปรแกรมจะเข้าใจว่าบรรทัดนั้นคือข้อมูล Footer  )

* หมายเหตุ -header, -body, -footer ไม่ใส่ก็ได้ โปรแกรมจะ Default ค่าไว้ให้ HT, DT, FT ตามลำดับ

"""

"""
 ไฟล์ Log จะเก็บอยู่ที่ Folder logs
"""