import glob
import json
import os.path
import codecs
import xml.etree.ElementTree as ET
import csv
import re
from pymongo import MongoClient

def classify_csv(file):
    CSV_question = ""
    CSV_answer = ""
    
    question_first_line = True
    answer_first_line = True
    
    tree = ET.parse(file)
    root = tree.getroot()
    #print(root)
    for record in tree.iter(tag='row'):
        r = record.attrib
        
        if int(r["PostTypeId"]) == 1: # 1 means question, 2 means answer
            r["Title"] = re.sub(r',', " ",r["Title"]) 
            if question_first_line:
                column_q = "Id,Title,CreationDate\n"
                CSV_question = CSV_question + column_q
                line_question = '{},{},{}\n'.format(r["Id"], str(r["Title"]), r["CreationDate"])
                question_first_line = False
            elif r["Id"] and r["Title"] and r["CreationDate"]:
                line_question = '{},{},{}\n'.format(r["Id"], str(r["Title"]), r["CreationDate"])
            
            CSV_question += line_question
            
        else:
            r["Body"] = re.sub(r'(<[^<]+>)|(\s+)|(,)', " ",r["Body"]) #remove tags, newlines, comma(conflict with , in csv)
            if answer_first_line:
                column_a = "Id,ParentId,CreationDate,Body,OwnerUserId\n"
                CSV_answer = CSV_answer + column_a
                line_answer = '{},{},{},{},{}\n'.format(r["Id"], r["ParentId"], r["CreationDate"], str(r["Body"]), r["OwnerUserId"])
                answer_first_line = False
            elif r.get("OwnerUserId") and r.get("Id") and r.get("ParentId") and r.get("CreationDate") and r.get("Body"):         
                line_answer = '{},{},{},{},{}\n'.format(r["Id"], r["ParentId"], r["CreationDate"], str(r["Body"]), r["OwnerUserId"])
            CSV_answer += line_answer
            

    with open("question.csv", "w") as file_q:
        file_q.write(CSV_question)  
    
    with open("answer.csv", "w") as file_a:
        file_a.write(CSV_answer)
            
def save2Mongodb(file):
    connection = MongoClient('localhost', 27017)
    db = connection['stackmongodb']
    collect_question = db["question"]
    collect_answer = db["answer"]

    #insert data
    tree = ET.parse(file)
    root = tree.getroot()
    #print(root)
    for record in tree.iter(tag='row'):
        r = record.attrib
        
        if int(r["PostTypeId"]) == 1: # 1 means question, 2 means answer
            r["Title"] = re.sub(r',', " ",r["Title"]) 
            if r.get("Id") and r.get("Title") and r.get("CreationDate"):
                collect_question.insert_one(
                    {
                        "Id":r["Id"],
                        "Title":str(r["Title"]),
                        "Body":str(r["Body"]),
                        "CreationDate":r["CreationDate"]
                    }
                )
            
        else:
            r["Body"] = re.sub(r'(<[^<]+>)|(\s+)|(,)', " ",r["Body"]) #remove tags, newlines, comma(conflict with , in csv)
            if r.get("OwnerUserId") and r.get("Id") and r.get("ParentId") and r.get("CreationDate") and r.get("Body"):         
                collect_answer.insert_one(
                    {
                        "Id":r["Id"],
                        "ParentId":str(r["ParentId"]),
                        "CreationDate":str(r["CreationDate"]),
                        "Body":r["Body"]
                    }
                )


def test():

    connection = MongoClient('localhost', 27017)
    db = connection['stackmongodb']
    collect_question = db["question"]

    result = collect_question.find_one({"Id": "8588"})
    print(result)
            

if __name__=="__main__":

    # cd ~/Applications/mongodb/bin  
    # ./mongod
    print("main")
    #classify_csv("/Users/xueguoliang/myGithub/BIg Data Science/programmers.stackexchange.com/Posts.xml") # it is in my local temporarily
    #save2Mongodb("/Users/xueguoliang/myGithub/BIg Data Science/programmers.stackexchange.com/Posts.xml")
    test()

