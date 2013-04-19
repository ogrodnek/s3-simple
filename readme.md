# S3 Simple

This project is a simple java library for connecting to Amazon S3.

It's the s3-shell code taken from: <http://developer.amazonwebservices.com/connect/entry.jspa?externalID=138&categoryID=47>

And a Java Base64 class taken from:
<http://iharder.sourceforge.net/current/java/base64/>

For general purpose S3 use, you really should be using Amazon's office [AWS Java SDK](http://aws.amazon.com/sdkforjava/) (github: <https://github.com/aws/aws-sdk-java>)

BUT, I needed something super simple that would work with the restrictions around google app engine... namely, no threads, no sockets, and... other restrictions...

So, this is the base s3-shell code, with the fix mentioned in the comments, and the Base64 class stolen from another aws example (s3-shell previously used a com.sun class, which you can't do in AE)...

The S3 support is pretty basic...

## Usage

```
S3Store s3 = new S3Store("s3.amazonaws.com", ACCESS_KEY, SECRET_KEY);
s3.setBucket("my-bucket");

// upload an item as public-read
s3.storeItem("test", new String("hello").getBytes(), "public-read");

// upload a js file, with a cache control-header
final Map<String, List<String>> headers = new HashMap<String, List<String>>();
headers.put("Cache-Control", Collections.singletonList("max-age=300, must-revalidate"));
headers.put("Content-Type", Collections.singletonList("application/x-javascript"));

s3.storeItem("test2.js", new String("document.write('hello');").getBytes(), "public-read", headers);
```