
from fastapi import Depends, FastAPI, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
#from .App import mysql_connection_test
from App import mysql_connection_test
from typing import Annotated, Any, Dict, AnyStr, List, Union, Optional
from enum import Enum
from pydantic import BaseModel
from App.mysql_connection_test import ConnectionHandler, SessionLocal
# from models import Worksheet, GeneralQuestion, ResponseWorksheet, QRQuestion
import pandas as pd
import json
from sqlalchemy.orm import Session
import uvicorn

#to run the app uvicorn api:app --reload

JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = Union[JSONArray, JSONObject]


class Module(str,Enum):
    AddProfile="add_profile"
    AddCourse="add_course"
    AddBatch="add_batch"
    AddStudent="add_student"
    AddTopic="add_topic"
    AddWorksheet = "add_worksheet"
    AddGeneralQuestion = "add_general_question"
    AddQuestionOption="add_question_option"
    AddStudentWorksheet="add_student_worksheet"
    AddGeneralQuestionResult="add_general_question_result"
    AddQRQuestion = "add_qr_question"
    AddQRQuestionResult="add_qr_question_result"

    AddStudentCourse="add_student_course"
    AddStudentTopic="add_student_topic"

    B4EditProfileById="b4_edit_profile_by_id"
    B4EditCourseById="b4_edit_course_by_id"
    B4EditStudentById="b4_edit_student_by_id"
    B4EditTopicById="b4_edit_topic_by_id"
    B4EditWorksheetById="b4_edit_worksheet_by_id"
    B4EditGeneralQuestionById="b4_edit_general_question_by_id"
    B4EditQuestionOptionById="b4_edit_question_option_by_id"
    B4EditStudentWorksheetById="b4_edit_student_worksheet_by_id"
    
    EditProfile="edit_profile"
    EditWorksheet="edit_worksheet"
    EditStudentWorksheet="edit_student_worksheet"
    EditGeneralQuestion="edit_general_question"
    EditQuestionOption="edit_question_option"
    DeleteProfile="delete_profile"
    DeleteQuestionOption="delete_question_option"
    GetAllProfiles="get_all_profile"
    GetAllCourses="get_all_courses"
    # GetProfileForId="get_profile_id"
    GetallTopics="get_all_topics"
    GetAllWorksheets="get_all_worksheets"

    GetAllActiveStudents="get_all_active_students"

    GetTopicForStudentId="get_pending_topic_for_student"
    GetUnAssignedWorksheetsForStudentId="get_unassigned_worksheets_to_assign"
    GetPracticseWorksheetsForStudentId="get_practicse_worksheets"
    GetAssignedWorksheetsForStudentId="get_assigned_worksheets_for_student"

    GetProfileForWhatsappNo="get_profile_for_whats_app_no"
    GetStudentsForProfileId="get_students_for_profile_id"


# class GetInfoModules(str,Enum):
#     GetProfiles="get_profile"


class GenericRequest(BaseModel):
    module:Module
    json_request:JSONStructure



app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:4200",
    "http://speedmaths.online"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#######  method only for ORM ########

# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# db_dependency = Annotated[Session, Depends(get_db)]
#######  method only for ORM ########
module_table_mapping = {"add_profile":{"table_name":"profile","success_msg":"Profile(s) Inserted Successfully"}
                        ,"add_course":{"table_name":"course","success_msg":"Course(s) Inserted Successfully"}
                        ,"add_batch":{"table_name":"batch","success_msg":"Batch(s) Inserted Successfully"}
                        ,"add_student":{"table_name":"student","success_msg":"Student(s) Inserted Successfully"}
                        ,"add_topic":{"table_name":"topic","success_msg":"Topic(s) Inserted Successfully"}
                        ,"add_worksheet":{"table_name":"worksheet","success_msg":"Worksheet(s) Inserted Successfully"}
                        ,"add_general_question":{"table_name":"general_question","success_msg":"General question(s) Inserted Successfully"}
                        ,"add_question_option":{"table_name":"question_option","success_msg":"General question option(s) Inserted Successfully"}
                        ,"add_student_worksheet":{"table_name":"student_worksheet","success_msg":"Student worksheet Inserted Successfully"}
                        ,"add_general_question_result":{"table_name":"general_question_result","success_msg":"Student worksheet results(s) Inserted Successfully"}
                        ,"add_qr_question":{"table_name":"qr_question","success_msg":"Quotient Reminder question(s) Inserted Successfully"}
                        ,"add_qr_question_result":{"table_name":"qr_question_result","success_msg":"Quotient Reminder Answer(s) Inserted Successfully"}
                        ,"add_student_course":{"table_name":"student_course","success_msg":"Student Course(s) Inserted Successfully"}
                        ,"add_student_topic":{"table_name":"student_topic","success_msg":"Student Topic(s) Inserted Successfully"}


                        ,"sample":{"table_name":"sample","success_msg":"Sample(s) Inserted Successfully"}
                        
                        ,"edit_profile":{"table_name":"profile","success_msg":"Profile Updated Successfully"}
                        ,"edit_worksheet":{"table_name":"worksheet","success_msg":"Worksheet Updated Successfully"}
                        ,"edit_student_worksheet":{"table_name":"student_worksheet","success_msg":"Student Worksheet Updated Successfully"}
                        ,"edit_general_question":{"table_name":"general_question","success_msg":"General Question Updated Successfully"}
                        ,"edit_question_option":{"table_name":"question_option","success_msg":"Question Option Updated Successfully"}
                        ,"get_all_topics":{"query":"select * from topic"}
                        ,"get_all_worksheets":{"query":"select * from worksheet"}
                        
                        ,"delete_profile":{"table_name":"test1","success_msg":"Profile(s) Deleted Successfully"}
                        ,"delete_question_option":{"table_name":"question_option","success_msg":"Question Option(s) Deleted Successfully"}
                        
                        ,"get_all_profile":{"query":"select * from profile"}
                        # ,"get_profile_id":{"query":"select * from profile where id="}
                        ,"get_all_courses":{"query":"select * from course"}
                        ,"get_all_active_students":{"query":"select * from student where is_active=1"}

                        ,"b4_edit_profile_by_id":{"table_name":"profile", "field_name":"id"}
                        ,"b4_edit_course_by_id":{"table_name":"course", "field_name":"id"}
                        ,"b4_edit_student_by_id":{"table_name":"student", "field_name":"id"}
                        ,"b4_edit_topic_by_id":{"table_name":"topic", "field_name":"id"}
                        ,"b4_edit_worksheet_by_id":{"table_name":"worksheet", "field_name":"id"}
                        ,"b4_edit_general_question_by_id":{"table_name":"general_question", "field_name":"id"}
                        ,"b4_edit_question_option_by_id":{"table_name":"question_option", "field_name":"id"}
                        ,"b4_edit_student_worksheet_by_id":{"table_name":"student_worksheet", "field_name":"id"}

                        ,"get_pending_topic_for_student":{"query":"select x.* from (select t.* from student_course sc inner join topic t on sc.course_id=t.course_id where sc.student_id={id} and sc.is_active=1) x left outer join (select t.* from student_topic st  inner join topic t on st.topic_id=t.id   where st.student_id={id}) y on x.id=y.id where y.id is null"}
                        ,"get_unassigned_worksheets_to_assign":{"query":"select c.id as course_id, c.code as course_code, c.name as course_name, c.is_vedic_maths, t.id as topic_id, t.name as topic_name, t.sequence,ws.id, ws.name, ws.type, ws.table_of from course c inner join topic t on c.id=t.course_id inner join student_topic st on st.topic_id=t.id inner join worksheet ws on ws.topic_id=t.id left outer join student_worksheet sw on sw.worksheet_id=ws.id where st.student_id={id} and is_practice=0 and sw.id is null;"}
                        ,"get_practicse_worksheets":{"query":"select c.id as course_id, c.code as course_code, c.name as course_name, c.is_vedic_maths, t.id as topic_id, t.name as topic_name, t.sequence,ws.id, ws.name, ws.type, ws.table_of from course c inner join topic t on c.id=t.course_id inner join student_topic st on st.topic_id=t.id and st.student_id={id} inner join worksheet ws on ws.topic_id=t.id where is_practice=1"}
                        ,"get_assigned_worksheets_for_student":{"query":"select c.id as course_id, c.code as course_code, c.name as course_name, c.is_vedic_maths, t.id as topic_id, t.name as topic_name, t.sequence,ws.id, sw.id as student_worksheet_id, sw.student_id, sw.assigned_date, ws.name, ws.type, ws.table_of from course c inner join topic t on c.id=t.course_id inner join student_topic st on st.topic_id=t.id inner join worksheet ws on ws.topic_id=t.id left outer join student_worksheet sw on sw.worksheet_id=ws.id where st.student_id={id} and is_practice=0;"}
                        
                        ,"get_profile_for_whats_app_no":{"query":"select p.id, p.whats_app_no, p.is_active, p.role_id, r.name as role_name from profile p left outer join role r on p.role_id=r.id where is_active=1 and whats_app_no like '%%{id}%%'"}
                        ,"get_students_for_profile_id":{"query":"select * from student where is_active=1 and profile_id={id}"}
                        ,"get_worksheets_by_id":{"query":"select * from worksheet where id={id}"}
                        }

@app.get('/')
def default():
    return 'default page'



@app.get('/profiles')
def get_profiles():
    dal = ConnectionHandler()
    df = dal.fetch_data("select * from profile")
    json_records = df.to_json(orient='records').splitlines()
    # df.reset_index(inplace=True)
    print(json_records)
    # return json_records
    return JSONResponse(content=json_records)
    # json_compatible_item_data = jsonable_encoder(df["json"])
    # return JSONResponse(content=json_compatible_item_data)

######################################################## Generic Get block all data #######################################################################
@app.get('/GetAllInfo')
def get_info(module:Module):
    print(f"module is {module}")
    query=module_table_mapping.get(module.value).get("query")
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print(json_records)
    return JSONResponse(content=json_records)

######################################################## Generic Get block all data #######################################################################

######################################################## Add block #######################################################################

@app.post('/add')
def add(module:Module,generic_json:GenericRequest):
    print(f"module is {module}, and json is {generic_json}")
    return_value = ""
    #print(module.name, module.value)
    table_name = module_table_mapping.get(module.value).get('table_name')
    #print(table_name)
    success_msg = module_table_mapping.get(module.value).get('success_msg')
    return_value = add_data(generic_json=generic_json,table_name=table_name,success_msg=success_msg)
    return return_value


def add_data(generic_json:GenericRequest, table_name:str,success_msg:str):
    json_data = generic_json.json_request
    df = pd.DataFrame.from_dict(json_data)
    dal = ConnectionHandler()
    dal.insert_data(df,table_name)
    return success_msg

######################################################## Add block #######################################################################

######################################################## Edit block #######################################################################

@app.post('/edit')
def edit(module:Module,generic_json:GenericRequest):
    print(f"module is {module}, and json is {generic_json}")
    return_value = ""
    #print(module.name, module.value)
    table_name = module_table_mapping.get(module.value).get('table_name')
    #print(table_name)
    success_msg = module_table_mapping.get(module.value).get('success_msg')
    return_value = edit_data(generic_json=generic_json,table_name=table_name,success_msg=success_msg)
    return return_value

def edit_data(generic_json:GenericRequest, table_name:str,success_msg:str):
    final_query=form_query(generic_json=generic_json,table_name=table_name)
    print(final_query)
    dal = ConnectionHandler()
    rowcount = dal.execute_query(final_query)
    print(rowcount)
    return success_msg

def form_query(generic_json:GenericRequest, table_name:str):
    edit_json=generic_json.json_request
    query_part1 = f"update {table_name} set "
    cols=""
    final_query=""
    where_condition=""
    for x in edit_json:
        
        field_name=x.get("field_name")
        field_value=x.get("field_value")
        field_type=x.get("field_type")
        if field_name =="id":
            where_condition=f" where id={x.get('field_value')}"
        else:
            if field_type in ['bit','int']:
                cols = cols+f" {field_name}={field_value},"
            else:
                cols = cols+f" {field_name}='{field_value}',"
    cols=cols[0:len(cols)-1]
    final_query=query_part1+cols+where_condition
    return final_query

######################################################## Edit block #######################################################################

######################################################## Delete block #######################################################################

@app.delete('/delete')
def delete(module:Module,generic_json:GenericRequest):
    print(f"module is {module}, and json is {generic_json}")
    return_value = ""
    #print(module.name, module.value)
    table_name = module_table_mapping.get(module.value).get('table_name')
    #print(table_name)
    success_msg = module_table_mapping.get(module.value).get('success_msg')
    return_value = delete_data(generic_json=generic_json,table_name=table_name,success_msg=success_msg)
    return return_value

def delete_data(generic_json:GenericRequest,table_name,success_msg):
    final_query = form_delete_query(generic_json=generic_json,table_name=table_name)
    dal = ConnectionHandler()
    rowcount = dal.execute_query(final_query)
    print(rowcount)
    return success_msg

def form_delete_query(generic_json:GenericRequest,table_name):
    delete_json=generic_json.json_request
    query_part1 = f"delete from {table_name} where id"
    final_query="" 
    where_condition=""
    len_delete_json = len(delete_json)
    print(len_delete_json)
    if len_delete_json==1:
        where_condition=f"={delete_json[0].get('id') }"
    elif len_delete_json>1:
        where_condition=f" in ("
        for x in delete_json:
            where_condition=where_condition+f"{x.get('id')} ,"
        where_condition=where_condition[0:len(where_condition)-1]
        where_condition=where_condition+")"

    final_query=query_part1+where_condition
    print(final_query)

    return final_query

######################################################## Delete block #######################################################################

######################################################## Get Data For Edit block #######################################################################

@app.post('/data_for_edit')
def data_for_edit(module:Module,generic_json:GenericRequest):
    print(f"module is {module}, and json is {generic_json}")
    print(generic_json.json_request)
    edit_json=generic_json.json_request
    table_name = module_table_mapping.get(module.value).get('table_name')
    value_from_request=edit_json.get("id")
    print(edit_json.get("id"))
    json_to_return = get_data_for_edit(table_name=table_name, id_field="id",id_field_value=value_from_request)
    return json_to_return

def get_data_for_edit(table_name, id_field, id_field_value):
    
    dal = ConnectionHandler()
    df_data = dal.fetch_data(f"select * from {table_name} where {id_field}={id_field_value}")
    data_json_records = df_data.to_json(orient='records', lines=True, index=False).splitlines()

    df_schemas = dal.fetch_data(f"SELECT COLUMN_NAME as field_name, DATA_TYPE as field_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sra_vm' AND TABLE_NAME ='{table_name}'")
    schema_json_records = df_schemas.to_json(orient='records',lines=True, index=False).splitlines()

    final_out_list = create_edit_json(data_json=data_json_records, schema_json=schema_json_records)
    return final_out_list

def create_edit_json(data_json, schema_json):
    my_data_json=json.loads(data_json[0])
    final_out_list = []
    for x in schema_json:
        temp_x=json.loads(x)
        #print(my_data_json[temp_x["field_name"]])
        temp_x["field_value"]=my_data_json[temp_x["field_name"]]
        final_out_list.append(temp_x)
    print(final_out_list)        
    return final_out_list


######################################################## Get Data For Edit block #######################################################################

######################################################## Get Data For ID block #######################################################################

# @app.get('/get_pending_topic_for_student')
# def get_pending_topic_for_student(id:int):
#     query = module_table_mapping.get("get_pending_topic_for_student").get("query")
#     query = query.replace("{id}",str(id))
#     print(query,id)
#     dal = ConnectionHandler()
#     df = dal.fetch_data(query=query)
#     print(df)

#     return 

@app.get('/get_data_for_id')
def get_data_for_id(module:Module,id:int):
    query = table_name = module_table_mapping.get(module.value).get('query')
    query = query.replace("{id}",str(id))
    print(query)
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print(json_records)
    return JSONResponse(content=json_records)
    

######################################################## Get Data For ID block #######################################################################
######################################################## Get Data like block #######################################################################

@app.get('/get_data_like')
def get_data_like(module:Module,id:str):
    query = table_name = module_table_mapping.get(module.value).get('query')
    print(query)
    query = query.replace("{id}",str(id))
    print(query)
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print(json_records)
    return JSONResponse(content=json_records)

######################################################## Get Data like block #######################################################################

# @app.get('/worksheet/{worksheet_id}')
# def get_worksheet_for_id(db: db_dependency,worksheet_id: int):
#     try:
#         worksheet_model = db.query(Worksheet).filter(Worksheet.id==worksheet_id).first()
#         print(worksheet_model.type_of_worksheet)
#         if worksheet_model.type_of_worksheet=="General":
#             general_question_model = db.query(GeneralQuestion).filter(GeneralQuestion.worksheet_id==worksheet_id).all()
#             for x in general_question_model:
#                 print(x.__dict__)
#                 my_temp_dict = x.__dict__
#                 del my_temp_dict['_sa_insatance_state']
#                 print(my_temp_dict)
#         elif worksheet_model.type_of_worksheet=="QR_Division":
#             qr_question_model = db.query(QRQuestion).filter(GeneralQuestion.worksheet_id==worksheet_id).all()
#             print(qr_question_model.__dict__)
#         worksheet_dict = worksheet_model.__dict__
#         print(worksheet_dict)
#         # print(general_question_model.__dict__)
#     except Exception as e:
#         print(f"something went wrong {e}")
#     return worksheet_model # , general_question_model

@app.get('/worksheet/{worksheet_id}')
def get_worksheet_for_id(worksheet_id:int=Path(ge=1)):
    query = table_name = module_table_mapping.get('get_worksheets_by_id').get('query')
    print(query)
    query = query.replace("{id}",str(worksheet_id))
    print(query)
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print('\n',json_records)
    my_temp_worksheet_dict = {}
    for x in json_records:
        my_temp_worksheet_dict = json.loads(x)
        if my_temp_worksheet_dict['type']=="General":
            my_temp_worksheet_dict['GeneralQuestions'] = get_general_questions_for_worksheet_id(worksheet_id=worksheet_id)

        elif my_temp_worksheet_dict['type']=="QR_Division":
            my_temp_worksheet_dict['QR_Division_Questions'] = get_qr_division_questions(worksheet_id=worksheet_id)
        else:
            continue

    # print(my_temp_worksheet_dict['type'])
    # if my_temp_worksheet_dict["type"]=="General":
    
    # my_worksheet_dict = json.loads(str(json_records))
    # print(my_worksheet_dict)
    return JSONResponse(content=my_temp_worksheet_dict)

def get_general_questions_for_worksheet_id(worksheet_id:str):
    
    query = f"select * from general_question where worksheet_id={worksheet_id}"
    
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print('\n',json_records)
    final_list = []
    for x in json_records:
        my_question_dict = json.loads(x)
        # print(my_question_dict, my_question_dict["type"])
        # if my_question_dict["type"]=="radio":
        #     my_question_dict["general_question_options"] = get_general_question_options(my_question_dict["id"])
        my_question_dict["general_question_options"] = get_general_question_options(my_question_dict["id"])
        final_list.append(my_question_dict)
    print('\n',my_question_dict)    
    print('\n',final_list)
    # return my_question_dict
    return final_list

def get_general_question_options(general_question_id:str):
    query = f"select * from question_option where general_question_id={general_question_id}"
    
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print('\n',json_records)
    return json_records

def get_qr_division_questions(worksheet_id:str):
    query = f"select * from qr_question where worksheet_id={worksheet_id}"
    dal = ConnectionHandler()
    df = dal.fetch_data(query=query)
    json_records = df.to_json(orient='records', lines=True, index=False).splitlines()
    print('\n',json_records)
    return json_records

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000,reload = True)