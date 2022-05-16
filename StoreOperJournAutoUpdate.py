# file <script.py>
import pyodbc

query ="SELECT TOP 10 * FROM [dwh_sandbox].[dbo].[EEC_StoreOperJourn]"

queryDel = "delete from dwh_sandbox.dbo.EEC_StoreOperJourn"

queryInsert = f"""use dwh_sandbox
insert into EEC_StoreOperJourn (STRJ_RN,STRJ_GOODSSUPPLY,STRJ_OPER_TYPE,STRJ_UNITCODE,STRJ_DOCTYPE,STRJ_OPERDATE,STRJ_QUANT,STRJ_STOPER,GS_PRN,GS_STORE,GS_NOMMODIF,
AZS_AZS_NUMBER,AZS_AZS_NAME,REG_Region,GP_Indoc,ICD_OH,AZT_GSMWAYS_MNEMO,AZT_GSMWAYS_NAME,DOC_DOCNAME,DOCLinks_OUT_DOC,DOCLinks_IN_DOC,
ACT_STORE,ACT_STORE_NUMBER,ACT_STORE_NAME,ACT_Store_reg,IN_STORE,IN_STORE_NUMBER,IN_STORE_NAME,in_Store_reg,in_Unitcode)
select STRJ.RN as STRJ_RN
	,STRJ.GOODSSUPPLY as STRJ_GOODSSUPPLY
	,STRJ.OPER_TYPE as STRJ_OPER_TYPE
	,STRJ.UNITCODE as STRJ_UNITCODE
	,STRJ.DOCTYPE as STRJ_Doctype
	,STRJ.OPERDATE as STRJ_OPERDATE
	,STRJ.QUANT as STRJ_QUANT
	,STRJ.STOPER as STRJ_Stoper

	,GS.PRN as GS_PRN
	,GS.STORE as GS_STORE
	,GS.NOMMODIF as GS_NOMMODIF

	,AZS.AZS_NUMBER as AZS_AZS_NUMBER
	,AZS.AZS_NAME as AZS_AZS_NAME

	,REG.[Складской комплекс] as REG_Region

	,gp.INDOC as GP_Indoc

	,ICD.STOR_SIGN as ICD_OH

	,AZT.GSMWAYS_MNEMO as AZT_GSMWAYS_MNEMO
	,AZT.GSMWAYS_NAME as AZT_GSMWAYS_NAME

	,DOC.DOCNAME as DOC_DOCNAME

	,DOCL.OUT_DOCUMENT as DOCLinks_OUT_DOC
	,DOCL.IN_DOCUMENT as DOCLinks_IN_DOC

	,TRANSDEP.store as ACT_STORE
	,AZS_ACTS.AZS_NUMBER as ACT_STORE_NUMBER
	,AZS_ACTS.AZS_NAME as ACT_STORE_NAME
	,REG_acts.[Складской комплекс] as ACT_Store_reg

	,TRANSDEP.IN_STORE as IN_STORE
	,AZS_INS.AZS_NUMBER as IN_STORE_NUMBER
	,azs_ins.AZS_NAME as IN_STORE_NAME
	,REG_in.[Складской комплекс] as in_Store_reg
	,docl.IN_UNITCODE as In_unitcode


from [dwh_PROD].[DWH].[V_STOREOPERJOURN] STRJ
left join [dwh_PROD].[DWH].[V_GOODSSUPPLY] GS on STRJ.GOODSSUPPLY = GS.RN
left join [dwh_PROD].[DWH].[V_AZSAZSLISTMT] AZS on GS.STORE = AZS.RN
left join [dwh_sandbox].[dbo].[mss_warehouses_log] REG on AZS_NUMBER = REG.[Номер]
left join [dwh_PROD].[DWH].[V_GOODSPARTIES]  GP on GS.PRN = GP.RN
left join [dwh_PROD].[DWH].[V_INCOMDOC] ICD on  GP.INDOC    = ICD.RN
left join [dwh_PROD].[DWH].[V_AZSGSMWAYSTYPES] AZT on STRJ.STOPER = AZT.RN
left join [dwh_PROD].[DWH].[V_DOCTYPES]  DOC on STRJ.DOCTYPE = DOC.rn

left join ( select * from [dwh_prod].[DWH].[V_DOCLINKS] 
where IN_UNITCODE not like '%Specs%')  DOCL on STRJ.RN = DOCL.OUT_DOCUMENT

left join [dwh_prod].[dwh].[V_TRANSINVDEPT] TRANSDEP on DOCL.IN_DOCUMENT = TRANSDEp.RN

left join [dwh_PROD].[DWH].[V_AZSAZSLISTMT] AZS_INS on TRANSDEP.IN_STORE = AZS_INS.RN
left join [dwh_sandbox].[dbo].[mss_warehouses_log] REG_IN on azs_ins.AZS_NUMBER = REG_in.[Номер]

left join [dwh_PROD].[DWH].[V_AZSAZSLISTMT] AZS_ACTS on TRANSDEP.STORE = AZS_ACTS.RN
left join [dwh_sandbox].[dbo].[mss_warehouses_log] REG_acts on azs_acts.AZS_NUMBER = REG_acts.[Номер]
where STRJ.OPERDATE >= '20210101'"""


print('Update started')
driver = '{SQL Server Native Client 11.0}'
dbname = 'dwh_sandbox'

with pyodbc.connect(f"Driver={driver}; Server=sa-vm-dwh; Database={dbname}; uid=maslov; pwd=maslov_pwd; Trusted_connection=no") as conn:
    with conn.cursor() as cursor:
        cursor.execute(queryDel)
        try:
            row = cursor.fetchone()
        except Exception as e:
            print(e)
            crs = conn.cursor()
            crs.close

with pyodbc.connect(f"Driver={driver}; Server=sa-vm-dwh; Database={dbname}; uid=maslov; pwd=maslov_pwd; Trusted_connection=no") as conn:
    with conn.cursor() as cursor:
        try:
            cursor.execute(queryInsert)
        except Exception as e:
            print(e)
            crs = conn.cursor()
            crs.close

with pyodbc.connect(f"Driver={driver}; Server=sa-vm-dwh; Database={dbname}; uid=maslov; pwd=maslov_pwd; Trusted_connection=no") as conn:
    with conn.cursor() as cursor:
        try :
            cursor.execute(query)
        except Exception as e:
            print(e)
            crs = conn.cursor()
            crs.close

        rowRes = conn.cursor()
        while rowRes:
            print(str(rowRes))
            rowRes = cursor.fetchone()
        crs = conn.cursor()
        crs.close