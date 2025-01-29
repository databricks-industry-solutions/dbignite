from pyspark.sql.types import *
from pyspark.sql.types import _infer_type
from  dbignite.fhir_mapping_model import FhirSchemaModel
from itertools import groupby, chain
from collections import ChainMap


class MappingManager():

    #
    # Mappings
    #
    def __init__(self, mappings, src_schema, em = None):
        self.mappings = mappings
        self.src_schema = src_schema
        if len([x.fhir_resource() for x in self.mappings]) > 1:
            Exception("Support for only 1 FHIR resource within a mapping at a time")
        self.em = em if em is not None else FhirEncoderManager()
        
    #
    # Given a tgt_resource type, return all encoded values 
    #
    def encode(self, row, fhirResourceType):
        data = [(resourceType, mappings) for resourceType, mappings in self.level(0) if resourceType[0] == fhirResourceType][0]
        return self.to_fhir(data[0], data[1], row.asDict())

    #
    # Given a target field, get the source mapping else none
    #  tgt = array of ["Patient","identifier","value"]
    #
    def get_src(self, tgt):
        return None if len([x for x in self.mappings if x.tgt == ".".join(tgt)]) == 0 else [x for x in self.mappings if x.tgt == ".".join(tgt)][0]



    #
    # Get the func needed to transform x to y
    #  @param tgt = ["Patient", "identifier", "value"]
    #  
    #
    def get_func(self, tgt):
        return self.em.get_encoder(
            src_type = None if self.get_src(tgt) is None else SchemaDataType.traverse_schema(self.get_src(tgt).src.split("."), self.src_schema),
            tgt_name = tgt
        )
    
    #
    # fhir_resources to be mapped 
    #
    def fhir_resource_list(self):
        return list(set([x.fhir_resource() for x in self.mappings]))

    """
    # Return a grouping of all resources that match at a level
    #  @param level = numeric value starting at 0 = FHIR_RESOURCE
    #
    #  @return - list of lists that are at the same level, each value is a tuples
    #    tuple - (<matcing fhir path>, <list of mapping objects matched>)
    #
       e.g. level=3 [(['Patient', 'identifier', 'value'], [<dbignite.writer.fhir_encoder.Mapping object at 0x104ef81f0>]),
                     (['Patient', 'identifier', 'system'], [<dbignite.writer.fhir_encoder.Mapping object at 0x104eaef40>])]

       e.g. level=2 [(['Patient', 'identifier'],
                [<dbignite.writer.fhir_encoder.Mapping object at 0x104ef81f0>,
                 <dbignite.writer.fhir_encoder.Mapping object at 0x104eaef40>])]

       e.g. level=2 [('Pateint', 'extension'],
                 <dbignite.writer.fhir_encoder.Mapping object at 0x104eaef40>]) 
                 <dbignite.writer.fhir_encoder.Mapping object at 0x104eaef40>])] 
    
    """
        
    #
    # Level number to match
    #  if mapping provided, only provide level that intersects with this mapping
    #
    def level(self, level, resources = None):
        mappings = resources if resources is not None else self.mappings
        return [(k,list(g)) for k,g, in groupby(mappings, lambda x: x.tgt.split(".")[:level+1]) if len(k) >= level+1]

    def to_fhir(self, tgt_prefix, resource_list, row_dict):
        field_name = tgt_prefix[-1:][0]
        #converting an individual field to fhir
        if len(resource_list) == 1 and ".".join(tgt_prefix) == resource_list[0].tgt: 
            return {
                field_name: (self.get_func(tgt_prefix).f(row_dict.get(resource_list[0].src)) if resource_list[0].hardcoded == False else resource_list[0].src ) 
            }
        #converting multiple fields to a single field in FHIR
        elif len(resource_list) > 1 and ".".join(tgt_prefix) == resource_list[0].tgt:
            return {
                field_name: self.em.get_encoder("array<" + _infer_type(type(resource_list[0].src).__name__).simpleString() + ">", tgt_prefix).f([row_dict.get(x.src) for x in resource_list if row_dict.get(x.src) is not None])
            }
        else:
            return { field_name: self.get_func(tgt_prefix).f(
                [self.to_fhir(prefix,resources, row_dict) for prefix,resources in self.level(len(tgt_prefix), resource_list)]
            )}

class Mapping():

    def __init__(self, src, tgt, hardcoded = False):
        self.src = src
        self.tgt = tgt
        self.hardcoded = hardcoded

    def fhir_resource(self):
        return self.tgt.split(".")[0]

    def __str__(self):
        return "src:" + str(self.src) +", tgt:" + (self.tgt)

#
# Holds logic for a single encoding
#
class FhirEncoder():

    #
    # @param src - source value class type
    # @param tgt - target value class type
    # @param one_to_one - exact match true/false
    # @param precision_loss - true/false if converting from source to target loses value
    #
    def __init__(self, one_to_one, precision_loss, f, default = ''):
        self.one_to_one = one_to_one
        self.precision_loss = precision_loss
        self.f = self.handle(f)
        self.default = default

    def handle(self, f):
        def wrapper_func(*args, **kw):
            try:
                return ('' if f(*args, **kw) == None else f(*args, **kw))
            except:
                return self.default
        return wrapper_func

#        
# Logic for all bindings in fhir translation logic
#
class FhirEncoderManager():
    """
    A class for converting values between different types.
    """

    #
    # @param map - dictionary of key/value pairs for encoding values through lambda functions
    # @parma override_encoders - override functions to run when encountering a tgt value,
    #    - e.g. patient.name.given
    #
    def __init__(self, map = None, override_encoders = {}, fhir_schema = FhirSchemaModel()):
        self.map = map if map is not None else self.DEFAULT_ENCODERS
        self.override_encoders = override_encoders
        self.fhir_schema = fhir_schema

    #src python binding, tgt Spark Schema.typeName() binding
    DEFAULT_ENCODERS = {
        "IDENTITY": FhirEncoder(True, False, lambda x: x),
        "string": {
            "string": FhirEncoder(True, False, lambda x: x),
            "integer": FhirEncoder(False, False, lambda x: int(x.strip())),
            "float": FhirEncoder(False, False, lambda x: float(x.strip())),
            "double": FhirEncoder(False, False, lambda x: float(x.strip())),
            "bool":  FhirEncoder(False, False, lambda x: bool(x.strip())),
            "array<string>": FhirEncoder(False, False, lambda x: [x])
        },
        "array<string>":{
            "string": FhirEncoder(False, False, lambda x: ','.join(x))
            },
        "integer": {
            "string": FhirEncoder(False, True, lambda x: str(x))
        },
        "struct": FhirEncoder(False, True, lambda l: dict(ChainMap(*l))),
        "array<struct>": FhirEncoder(False, True, lambda l: [dict(ChainMap(*l))] )  #default behavior to union dictionary
    }


    #
    # Get encoders for src->tgt type. If src_type is None, default to just getting tgt type
    #
    def _get_encoder(self, src_type, tgt_type):
        return self.map.get("IDENTITY") if src_type == tgt_type else self.map.get(src_type, {}).get(tgt_type, self.map.get(tgt_type))  

    #
    # @param tgt_name - target field name
    # @param src_type - the source spark data type 
    #
    def get_encoder(self, src_type, tgt_name):
        return ( self.override_encoders.get('.'.join(tgt_name), None) 
                if self.override_encoders.get('.'.join(tgt_name), None) is not None
                else self._get_encoder(src_type, SchemaDataType.traverse_schema(tgt_name[1:], self.fhir_schema.schema(tgt_name[0]))))

class SchemaDataType:

    #
    # field = List of.. ["Patient"
    #
    @staticmethod
    def traverse_schema(field, struct):
        if not field and type(struct) != StructField and type(struct) != StructType:
            return struct.dataType
        elif not field and type(struct) == StructField:
            if struct.dataType.typeName() == "array":
                return "array<" + struct.dataType.elementType.typeName()  +">"
            return struct.dataType.typeName()
        elif not field and type(struct) == StructType:
            return "struct"
        else:
            if type(struct) == StructType:
                return SchemaDataType.traverse_schema(field[1:], struct[field[0]])
            elif type(struct) == StructField:
                return SchemaDataType.traverse_schema(field, struct.dataType)
            elif type(struct) == ArrayType:
                return SchemaDataType.traverse_schema(field, struct.elementType)

    @staticmethod
    def schema_to_python(schema):
        return schema.simpleString()

    
