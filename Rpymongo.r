mongodb.connect=function(dbName, collection, host, port, verbose=FALSE){
  rPython::python.exec("import pymongo")
  rPython::python.exec("import datetime")
  rPython::python.exec("from pymongo import MongoClient")
  rPython::python.exec(paste("client = MongoClient('",host,"', ",port,")",sep=""))
  rPython::python.exec(paste("db = client['",dbName,"']",sep=""))
  rPython::python.exec(paste("collection = db['",collection,"']",sep=""))
  
  print(paste("Connected to collection:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))
}
#mongodb.connect(dbName, collection, host, port)



mongodb.insert=function(doc_or_docs,verbose=FALSE) {
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))
  
  return(rPython::python.get(paste("collection.insert(",doc_or_docs,")",sep="")))
}



mongodb.save=function(to_save,verbose=FALSE) {
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))
  
  return(rPython::python.get(paste("collection.save(",to_save,")",sep="")))
}



mongodb.find=function(spec=NULL, verbose=FALSE, as.data.frame=FALSE) {
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))
  
  rPython::python.exec("from bson import Binary, Code")
  rPython::python.exec("from bson.json_util import dumps")
  
  if (is.null(spec))
    rPython::python.exec("r = collection.find()")
  else
    rPython::python.exec(paste("r = collection.find(spec=",spec,")",sep=""))
  
  resultJSON = rPython::python.get("dumps(list(r))")
  
  if (as.data.frame)
    return(do.call(rbind, lapply(fromJSON(resultJSON),as.data.frame)))
  else
    return(resultJSON)
}
#cat(mongodb.find(verbose=TRUE))
#mongodb.find(verbose=TRUE,as.data.frame=TRUE)




mongodb.remove=function(spec_or_id) {
  return(invisible(rPython::python.get(paste("collection.remove(",spec_or_id,")",sep=""))))
}
#mongodb.remove(spec_or_id=3) # by _id
#mongodb.remove(spec_or_id="{ 'name' : \"aNameToBeRemoved\"}") # by spec
#mongodb.remove(spec_or_id="{ 'name' : 'CLX15 Comdty'}") # by spec



mongodb.create_index=function(, key_or_list, cache_for=300) {
  result(invisible(rPython::python.get(paste("collection.create_index(",key_or_list,", cache_for=",cache_for,")",sep="")))
}

