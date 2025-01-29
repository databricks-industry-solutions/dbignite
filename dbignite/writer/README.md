# Writing FHIR Examples

## Using different versions of FHIR

```python
em = FhirEncoderManager(..., fhir_schema = FhirSchemaModel(schema_version="r4") #other versions are "r5", and "ci-build" 
```

## Source Data

Dataframes are the source for converting to FHIR. This can be a single table or a transformed table from a SQL Statement

```python
#Read in a DataFrame and convert to FHIR. Using dummy data with claim id, procedure codes, and diagnosis codes.
from dbignite.writer.bundler import *
from dbignite.writer.fhir_encoder import *
import json

data = spark.createDataFrame(
[('CLM1', 'PRCDR11', 'PRCDR12', 'PRCDR13', 'DX11', 'DX12', 'DX13'),
 ('CLM1', 'PRCDR21', 'PRCDR22', 'PRCDR23', 'DX21', 'DX22', 'DX23')],
['CLAIM_ID', 'PRCDR_CD1', 'PRCDR_CD2', 'PRCDR_CD3', 'DX_CD1', 'DX_CD2', 'DX_CD3'])
"""
This command could also be
data = spark.sql("SELECT CLM_ID, PRCDR_CD1, PRCDR_CD2, PRCDR_CD3, DX_CD1, DX_CD2, DX_CD3 FROM...")
"""
```

## How do transformations happen without writing code? 

Transformations are sourced from a set shown [here](https://github.com/databrickslabs/dbignite/blob/main/dbignite/writer/fhir_encoder.py#L155-L173). This is map of source data type to target data type and the explicit lambda function that performs the action

e.g. an array of strings is mapped to a string by the default behavior of [concatening arrays with a comma](https://github.com/databrickslabs/dbignite/blob/main/dbignite/writer/fhir_encoder.py#L165-L167)
```python
FhirEncoder(False, False, lambda x: ','.join(x))
```

e.g. to demonstrate an array of values mapping to a single string, can do the following
```python
maps = [
	Mapping('PRCDR_CD1', 'Claim.procedure.procedureCodeableConcept.coding.code'),
	Mapping('PRCDR_CD2', 'Claim.procedure.procedureCodeableConcept.coding.code'),
	Mapping('PRCDR_CD3', 'Claim.procedure.procedureCodeableConcept.coding.code')]

m = MappingManager(maps, data.schema)
b = Bundle(m)
b.df_to_fhir(data).map(lambda x: json.loads(x)).foreach(lambda x: print(json.dumps(x, indent=4)))


"""
{..."resourceType": "Bundle", ...
	"coding":[{
	-->	"code": "PRCDR21,PRCDR22,PRCDR23"
	}]
...}
"""
```

## Specifying transformations

However, each code should be it's own value in the "coding" array and not as one single value. I can extend the lambda framework with specifying the transformation at the target column, e.g. 

```python
#maps...
em = FhirEncoderManager(
  override_encoders ={
    "Claim.procedure.procedureCodeableConcept.coding": 
      FhirEncoder(False, False, lambda x: [{"code": y} for y in x[0].get("code").split(",")])
})
"""
 ^^ Run this function instead when building values under "coding".
x =  [ {"code": "PRCDR21,PRCDR22,PRCDR23"} ]
x[0].get("code").split(",") -> ['PRCDR21', 'PRCDR22', 'PRCDR23']
lambda returns -> [{'code': 'PRCDR21'}, {'code': 'PRCDR22'}, {'code': 'PRCDR23'}]
"""


m = MappingManager(maps, data.schema, em) 
b = Bundle(m)
b.df_to_fhir(data).map(lambda x: json.loads(x)).foreach(lambda x: print(json.dumps(x, indent=4)))
"""
{..."resourceType": "Bundle", ...
  "coding": [
    { "code": "PRCDR21" },
    { "code": "PRCDR22" },
    { "code": "PRCDR23" }
  ]
}
"""
```

This default transformations can be overriden or more data types can be added. Here we default strings to concatenate by newline characters instead of commas
```python
em = FhirEncoderManager()
em.DEFAULT_ENCODERS['array<string>']['string'] = FhirEncoder(False, False, lambda x: '\n'.join(x))

m = MappingManager(maps, data.schema, em) 
b = Bundle(m)
b.df_to_fhir(data).map(lambda x: json.loads(x)).foreach(lambda x: print(json.dumps(x, indent=4)))
"""
Since we're separating out by newline, notice the "coding" custom transformation no longer splits the codes out into separate arrays since it is splitting by "," and not "\n"
"coding": [{
  "code": "PRCDR21\nPRCDR22\nPRCDR23"
}]
"""

#Update the encoding manager to split by newline and add a "system" value to indicate this is a HCPCS code
em = FhirEncoderManager(
  override_encoders ={
    "Claim.procedure.procedureCodeableConcept.coding": 
      FhirEncoder(False, False, lambda x:
        [{"code": y,
          "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"} 
            for y in x[0].get("code").split("\n")])
  })
m = MappingManager(maps, data.schema, em) 
b = Bundle(m)
b.df_to_fhir(data).map(lambda x: json.loads(x)).foreach(lambda x: print(json.dumps(x, indent=4)))

"""
"coding": [{
  "code": "PRCDR11",
  "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"
},
{
  "code": "PRCDR12",
  "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"
},
{
  "code": "PRCDR13",
  "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"
}]
"""
```

## Complex Extensions

Extensions typically involve mapping multiple elements into an array together. The challenge is that in addition to mapping into an array, elements in the array are typically sub grouped together as an object in an array.

e.g. "code1" + "system1" + "code2" + "system2" -> [{"code1", "system1"}, {"code2", "system2"}]

This can be done in either the Dataframe creation step or lambda encoder.

```python
from pyspark.sql.functions import *
data.createOrReplaceTempView("claims")
df = spark.sql("""
SELECT map('code', prcdr_cd1, 'system', 'http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets') as codeset_prcdr,
map('code', dx_cd1, 'system', 'http://terminology.hl7.org/CodeSystem/icd9cm') as codeset_dx
from claims
""")
df.show(truncate=False)
"""
{valuecode -> PRCDR11, url -> http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets} , {valuecode -> DX11, url -> http://terminology.hl7.org/CodeSystem/icd9cm}
{valuecode -> PRCDR21, url -> http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets} , {valuecode -> DX21, url -> http://terminology.hl7.org/CodeSystem/icd9cm}
"""

maps = [
	Mapping('codeset_prcdr', 'Claim.extension'),
	Mapping('codeset_dx', 'Claim.extension')]

em = FhirEncoderManager(override_encoders ={
    "Claim.extension": 
      FhirEncoder(False, False, lambda x: x )})

m = MappingManager(maps, df.schema, em) 
b = Bundle(m)
b.df_to_fhir(df).map(lambda x: json.loads(x)).foreach(lambda x: print(json.dumps(x, indent=4)))

"""
 "extension": [
{
  "code": "PRCDR11",
  "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"
},
{
  "code": "DX11",
  "system": "http://terminology.hl7.org/CodeSystem/icd9cm"
}
]
"""
```

```python
TODO custom lambda function
```


# Common Errors

## Encoder doesn't exist yet

1. This means there does not exist a DEFAULT_ENCODER for one of the fields in the mapping source to target. Extend FhirEncoderManager with a function of either override_encoders or add to default encoderes to resolve
AttributeError: 'dict' object has no attribute 'f'

2. 

## Encoder data type doesn't match expected value
