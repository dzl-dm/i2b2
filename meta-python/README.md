# CoMetaR - generate sql statements for i2b2 from fuseki through SPARQL queries

Recursively writes statements for the concept and it's child-concepts.

* Every concept gets an entry in the i2b2 table.
* Every schema-topconcept also gets an entry in the table-access table.
* Every concept with notation gets an entry (for every notation) in the concept_dimension table.
- In case that a concept has multiple notations it will be marked with visualAttribute="MA" and for all notations there will be written another INSERT statement one i2b2-path level below. e.g. 
	* \ancestor path\concept\ visualAttribute="MA" notation="NULL"
	* \ancestor path\concept\0\ visualAttribute="LA" notation="'notation0'"
	* \ancestor path\concept\1\ visualAttribute="LA" notation="'notation1'"
* In case that a concept additionally has children another virtual path layer will be inserted. e.g. 
	* \ancestor path\concept\ visualAttribute="FA" notation="NULL"
	* \ancestor path\concept\multiple notations\ visualAttribute="MA" notation="NULL"
	* \ancestor path\concept\multiple notations\0\ visualAttribute="LA"  notation="'notation0'"
	* \ancestor path\concept\multiple notations\1\ visualAttribute="LA" notation="'notation1'"
	* \ancestor path\concept\child concept\	


## Process
This works in tandem with a CoMetaR instance which must perform certain things before triggering this process to complete the whole process.

CoMetaR must ensure the data is updated on its test fuseki server. Then notify this system
* This is done with `post_receive_hook.sh` and `add_files_to_dataset.sh`
* Notifying the newly separated part would need to be run after these parts - a simple curl to the i2b2 api component sounds sensible

Here, we must first pull the data from fuseki. Then process it into SQL statements which can be run against the i2b2 database to update the metadata.
* This was done by `export_rdf.sh` and `write-sql.sh` utilising the java code in this directory
* Exactly how this should proceed with a more modular docker process is currently unclear
* Should be part of the API component - or possibly only called by it, if its big enough to split out

