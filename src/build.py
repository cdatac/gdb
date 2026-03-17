import json
data=[{'sector':'Education','score':85},{'sector':'Health','score':72}]
open('docs/data.json','w').write(json.dumps(data))