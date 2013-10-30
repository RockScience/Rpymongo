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


mongodb.insert=function(doc_or_docs, manipulate=TRUE, safe="None", check_keys=TRUE, continue_on_error=FALSE, verbose=FALSE) {
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))

  # conversion of arguments to python format
  L = formals(mongodb.insert)
  L=lapply(L, FUN=function(x) if (class(x)=="logical") capitalize(tolower(x)) else x)
  L=L[setdiff(names(L),"verbose")]
  myArgs = paste(paste(paste(names(L),rep("=",length(L))),as.character(L)),collapse=", ") 
  
  return(rPython::python.get(paste("collection.insert(",myArgs,")",sep="")))
}



mongodb.save=function(to_save, manipulate=TRUE, safe="None", check_keys=TRUE, verbose=FALSE) {
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))
  
  # conversion of arguments to python format
  L = formals(mongodb.save)
  L=lapply(L, FUN=function(x) if (class(x)=="logical") capitalize(tolower(x)) else x)
  L=L[setdiff(names(L),"verbose")]
  myArgs = paste(paste(paste(names(L),rep("=",length(L))),as.character(L)),collapse=", ") 
  
  return(rPython::python.get(paste("collection.save(",myArgs,")",sep="")))
}



mongodb.find=function(spec="None", 
                      fields="None", 
                      skip=0, 
                      limit=0, 
                      timeout="True", 
                      snapshot=FALSE, 
                      tailable=FALSE, 
                      sort="None", 
                      max_scan="None",
                      as_class="None",
                      slave_okay=FALSE,
                      await_data=FALSE,
                      partial=FALSE, 
                      manipulate=TRUE, 
                      #read_preference="ReadPreference.PRIMARY", 
                      read_preference="pymongo.read_preferences.ReadPreference.PRIMARY",
                      exhaust=FALSE, 
                      verbose=FALSE, 
                      as.data.frame=FALSE) {
  #browser()
  
  # conversion of arguments to python format
  L = formals(mongodb.find)
  L=lapply(L, FUN=function(x) if (class(x)=="logical") capitalize(tolower(x)) else x)
  L=L[setdiff(names(L),"as.data.frame")]
  myArgs = paste(paste(paste(names(L),rep("=",length(L))),as.character(L)),collapse=", ") 
  
  if (verbose)
    print(paste("collection full name:", rPython::python.get("collection._Collection__full_name.encode('ascii')")))

  rPython::python.exec(paste("r = collection.find(",myArgs,")",sep=""))
  
  # Reference here: http://api.mongodb.org/python/current/api/bson/json_util.html
  rPython::python.exec("from bson import Binary, Code")
  rPython::python.exec("from bson.json_util import dumps")
  resultJSON = rPython::python.get("dumps(list(r))")
  
  if (as.data.frame)
    return(do.call(rbind, lapply(fromJSON(resultJSON),as.data.frame)))
  else
    return(resultJSON)
}
#cat(mongodb.find())
#fromJSON(mongodb.find())
#mongodb.find(as.data.frame=TRUE)


mongodb.remove=function(spec_or_id) {
  return(invisible(rPython::python.get(paste("collection.remove(",spec_or_id,")",sep=""))))
}w
#mongodb.remove(spec_or_id=3) # by _id
#mongodb.remove(spec_or_id="{ 'name' : \"aNameToBeRemoved\"}") # by spec
#mongodb.remove(spec_or_id="{ 'name' : 'CLX15 Comdty'}") # by spec


mongodb.dropCollection=function(name_or_collection, prompt=TRUE) {
  if (prompt){
    message=paste("You are about to drop the following collection: '", rPython::python.get("collection._Collection__full_name.encode('ascii')"),"'.\nThis action is not reversible, do you still want to proceed? (y/n) ",sep="")
    nb=readline(message)
    if (!identical(nb,"y")) return(NULL)
  }
  
  return(rPython::python.get(paste("collection.save(",name_or_collection,")",sep="")))
}


mongodb.create_index=function(key_or_list, cache_for=300) {
  res = rPython::python.get(paste("collection.create_index(", key_or_list, ", cache_for=" ,cache_for,")",sep=""))
  return(invisible(res))
}


mongodb.closeConnectionPool=function(){
  #ends all connections to the connection pool
  rPython::python.get("client.disconnect()")
}

mongodb.alive=function(){
  #checks if a connection can be established with the specified host+port
  rPython::python.get("client.alive()")
}
