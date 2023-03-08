import ujson
import json


json.loads(ujson.dumps("<html>\r\n  <head>\r\n    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0\">\r\n    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">\r\n    <style type=\"text/css\">\r\n      ReadMsgBody{ width: 100%;}\r\n      .ExternalClass {width: 100%;}\r\n      .ExternalClass, .ExternalClass p, .ExternalClass span, .ExternalClass font, .ExternalClass td, .ExternalClass div {line-height: 100%;}\r\n      body {-webkit-text-size-adjust:100%; -ms-text-size-adjust:100%;margin:0 !important;}\r\n      p { margin: 1em 0;}\r\n      t",encode_html_chars=True))