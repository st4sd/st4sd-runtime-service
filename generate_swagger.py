#!/usr/bin/env python
#
#  Copyright 2017 Otto Seiskari
#  Licensed under the Apache License, Version 2.0.
#  See http://www.apache.org/licenses/LICENSE-2.0 for the full text.
#
#  This file is based on
#  https://github.com/swagger-api/swagger-ui/blob/4f1772f6544699bc748299bd65f7ae2112777abc/dist/index.html
#  (Copyright 2017 SmartBear Software, Licensed under Apache 2.0)
#
# Updated in 2022 by Vassilis Vassiliadis to automatically export the HTML file of the
# Consumable Computing REST-API

"""VV: Instructions:
Place this file in the root directory of st4sd-runtime-service, activate a virtual environment
that has st4sd-runtime-service installed and then execute the file.

It will print the HTML code of the swagger-ui on its stdout.

**NOTE**: Different parts of the st4sd-runtime-service may print to the stderr so be careful to only capture
the stdout stream.
"""

import json
from app import api, app
import sys

app.config["SERVER_NAME"] = "localhost"
app.app_context().__enter__()

TEMPLATE = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Consumable Computing API</title>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans:400,700|Source+Code+Pro:300,600|Titillium+Web:400,600,700" rel="stylesheet">
  <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui.css" >
  <style>
    html
    {{
      box-sizing: border-box;
      overflow: -moz-scrollbars-vertical;
      overflow-y: scroll;
    }}
    *,
    *:before,
    *:after
    {{
      box-sizing: inherit;
    }}

    body {{
      margin:0;
      background: #fafafa;
    }}
  </style>
</head>
<body>

<div id="swagger-ui"></div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.9.0/swagger-ui-bundle.js"> </script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.9.0/swagger-ui-standalone-preset.js"> </script>
<script>
window.onload = function() {{

  var spec = {json.dumps(api.__schema__)};

  // Build a system
  const ui = SwaggerUIBundle({{
    spec: spec,
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout"
  }})

  window.ui = ui
}}
</script>
</body>

</html>
"""

print(TEMPLATE)
