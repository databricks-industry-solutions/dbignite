# Databricks notebook source
# MAGIC %md # Setup

# COMMAND ----------

# DBTITLE 1,Schema Setup
# MAGIC %sql
# MAGIC  CREATE CATALOG IF NOT EXISTS `quickstart_catalog_vkm_external`;
# MAGIC  CREATE SCHEMA IF NOT EXISTS `quickstart_catalog_vkm_external`.`ccda`;
# MAGIC  USE `quickstart_catalog_vkm_external`.`ccda`;

# COMMAND ----------

mkdir -p /dbfs/user/hive/warehouse/hls/fhir/raw_files_stg_checkpoint/

# COMMAND ----------

# DBTITLE 1,Download
# MAGIC %sh
# MAGIC mkdir -p /dbfs/user/hive/warehouse/hls/ccda/raw_files/
# MAGIC wget -O /dbfs/user/hive/warehouse/hls/ccda/raw_files/synthea_sample_data_ccda_latest.zip  https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_ccda_latest.zip 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unzip
# MAGIC %sh
# MAGIC unzip -d /dbfs/user/hive/warehouse/hls/ccda/raw_files/ /dbfs/user/hive/warehouse/hls/ccda/raw_files/synthea_sample_data_ccda_latest.zip
# MAGIC

# COMMAND ----------

# DBTITLE 1,Save to Delta
data_path = 'dbfs:/user/hive/warehouse/hls/ccda/raw_files/*xml'
(
  spark
  .read
  .format("xml")
  .option("rowTag", "ClinicalDocument")
  .load(data_path)
  .write
  .mode("overwrite")
  .saveAsTable('quickstart_catalog_vkm_external.ccda.ccda_bronze')
)

# COMMAND ----------

# MAGIC %md # CDA to FHIR

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databricks-industry-solutions/dbignite-forked.git

# COMMAND ----------

from dbignite.writer.fhir_encoder import *
from dbignite.writer.bundler import *
from  dbignite.fhir_mapping_model import FhirSchemaModel
import json

# COMMAND ----------

# MAGIC %md ## CCDA to FHIR Mapping

# COMMAND ----------

# DBTITLE 1,Patient Mapping
df = spark.sql("""
--Patient info
SELECT recordTarget.patientRole.id._root as patient_system
,"urn:ietf:rfc:3986" as patient_value
,recordTarget.patientRole.addr.city as patient_city
,NVL(recordTarget.patientRole.addr.postalCode, '') as patient_postal_code
,recordTarget.patientRole.addr.state as patient_state
,recordTarget.patientRole.addr.streetAddressLine as patient_streetAddressLine
,case 
  when recordTarget.patientRole.addr._use in ('H', 'HP', 'HV') THEN 'home'
  when recordTarget.patientRole.addr._use in ('WP', 'DIR', 'PUB') THEN 'work'
  when recordTarget.patientRole.addr._use in ('TMP') then 'temp'
  when recordTarget.patientRole.addr._use in ('OLD', 'BAD') then 'old'
  ELSE ''
END as patient_addrUse
,recordTarget.patientRole.patient.name.family as patient_familyName
,recordTarget.patientRole.patient.name.given as patient_givenName
,case 
  WHEN recordTarget.patientRole.patient.administrativeGenderCode._code == 'M' then 'male'
  WHEN recordTarget.patientRole.patient.administrativeGenderCode._code == 'F' then 'female'
  ELSE 'other' 
END as patient_gender
,cast(date_format(to_date(recordTarget.patientRole.patient.birthTime._value, 'yyyyMMddHHmmss'), 'yyyy-MM-ddHH:mm:ss') as string) as patient_birthTimeValue 
,ARRAY(MAP('url', 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
  'system', recordTarget.patientRole.patient.raceCode._codeSystem,
  'code', recordTarget.patientRole.patient.raceCode._code,
  'display', recordTarget.patientRole.patient.raceCode._displayName,
  'text', 'race'
)) as patient_raceCodeExt
FROM  quickstart_catalog_vkm_external.ccda.ccda_bronze
""")

# COMMAND ----------

#https://build.fhir.org/ig/HL7/ccda-on-fhir/CF-patient.html
patient_maps =[ 
    Mapping('patient_system', 'Patient.identifier.system'),
    Mapping('patient_value', 'Patient.identifier.value'),
    Mapping('patient_city', 'Patient.address.city'),
    Mapping('patient_postal_code', 'Patient.address.postalCode'),
    Mapping('patient_state', 'Patient.address.state'),
    Mapping('patient_streetAddressLine', 'Patient.address.line'),
    Mapping('patient_addrUse', 'Patient.address.use'),
    Mapping('patient_familyName', 'Patient.name.family'),
    Mapping('patient_givenName', 'Patient.name.given'),
    Mapping('patient_gender', 'Patient.gender'),
    Mapping('patient_birthTimeValue', 'Patient.birthDate'),
    Mapping('patient_raceCodeExt', 'Patient.extension')
]

em = FhirEncoderManager(override_encoders =
    {
        "Patient.extension": 
        FhirEncoder(False, False, lambda x: x )
    },
    fhir_schema = FhirSchemaModel(schema_version="r4") 
)

m = MappingManager(patient_maps, df.schema, em)
b = Bundle(m)
result = b.df_to_fhir(df)
#pretty print 10 values
print('\n'.join([str(x) for x in 
       result.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(10)]))

# COMMAND ----------

# DBTITLE 1,Procedure Mapping
df = spark.sql("""
--Procedures are an embedded resource. this filter command un-embeds the resource list
select cast(entry.procedure.code._code as string) as proc_code
,entry.procedure.statusCode._code as proc_status
,entry.procedure.code._codeSystem as proc_codeSystem
,entry.procedure.id._root as proc_id
,entry.procedure.templateId._root as proc_system
,cast(date_format(to_date(entry.procedure.effectiveTime._value , 'yyyyMMddHHmmss'), 'yyyy-MM-ddHH:mm:ss') as string)  as proc_performedDateTime
,NVL(entry.procedure.code._displayName, '') as proc_displayName
,patient_id
from (
select explode(filter(component.structuredBody.component.section, c -> c.title = "Surgeries")[0].entry) as entry
,recordTarget.patientRole.id._root as patient_id
from quickstart_catalog_vkm_external.ccda.ccda_bronze
)
""")

# COMMAND ----------

#https://build.fhir.org/ig/HL7/ccda-on-fhir/CF-procedures.html
procedure_maps =[ 
    Mapping('proc_status', 'Procedure.status'),
    Mapping('proc_code', 'Procedure.code.coding'),
    Mapping('proc_codeSystem', 'Procedure.code.coding'),
    Mapping('proc_displayName', 'Procedure.code.coding'),
    Mapping('proc_id', 'Procedure.identifier.value'),
    Mapping('proc_system', 'Procedure.identifier.system'),
    Mapping('proc_performedDateTime', 'Procedure.performedDateTime'),
    Mapping('patient_id', 'Procedure.subject')
]

em = FhirEncoderManager(override_encoders =
    {
        "Procedure.code.coding": 
        FhirEncoder(False, False, lambda x: [
            {
              'code': x[0],
              'system': x[1],
              'text': x[2]
            }
          ]),
        "Procedure.subject":  FhirEncoder(False, False, lambda x: {'reference': 'Patient/' +x})
    }, fhir_schema = FhirSchemaModel(schema_version="r4") 
)

m = MappingManager(procedure_maps, df.schema, em)
b = Bundle(m)
result = b.df_to_fhir(df)
#pretty print 10 values
print('\n'.join([str(x) for x in 
       result.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(10)]))