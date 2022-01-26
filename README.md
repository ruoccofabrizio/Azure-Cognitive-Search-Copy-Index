# Azure-Cognitive-Search-Copy-Index

Sample python code to copy Azure Cognitive Search indexes

## How to run it
  python main.py --parameters

| Parameter                                        | Value                | Note                                    |
|----------------------------------------------- |---------------------------------------------|--------------------------------------------|
| --src_service          			| SOURCE SERVICE 	 | Required									      |
| --src_service_key      			| SOURCE SERVICE KEY 	 | Required                                       |
| --src_index            			| SOURCE INDEX 		    | Required                                       |
| --dst_service          			| DESTINATION SERVICE         	 | Leave empty if you want to copy to src_service or dump the index to blob storage |
| --dst_service_key      			| DESTINATION SERVICE KEY   	 | Leave empty if you want to copy to src_service or dump the index to blob storage |
| --dst_index            			| DESTINATION INDEX 			 | Leave empty if you want to dump the index to blob storage |
| --filter_by            			| String filterable field used to batch read/write operation | Required                                       |
| --dump_on_blob            	| Connection String for Azure Blob Storage for index dump | Leave empty if you want to copy the index to another search service |
| --container_name            | Container name for Azure Blob Storage for index dump | Leave empty if you want to copy the index to another search service                                       |
