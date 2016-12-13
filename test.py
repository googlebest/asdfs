import json
import hashlib
import sys
from virus_total_apis import PublicApi as VirusTotalPublicApi
print len(sys.argv)
if len(sys.argv) > 1:
	API_KEY = '17e6fd95f55155460ab21ae697f2469511f9f834050e4fa2eb93b2987ffc26ed'

	EICAR = "X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
	EICAR_MD5 = sys.argv[1]
	
	vt = VirusTotalPublicApi(API_KEY)
	
	response =  vt.get_file_report(EICAR_MD5)
	print json.dumps(response, sort_keys=False, indent=4)