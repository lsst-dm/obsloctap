felis create --engine-url="postgresql://postgres:123456@127.0.0.1:5432/dummy"  --dry-run ../sdm_schemas/yml/obsloctap.yaml  | sed 's/ivoa.//g;s/\"//g' 
