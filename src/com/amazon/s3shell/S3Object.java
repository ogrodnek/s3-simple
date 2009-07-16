package com.amazon.s3shell;

import java.util.List;
import java.util.Map;

public final class S3Object {
  private final byte[] data;
  private final Map<String, List<String>> headers;
  
  public S3Object(final byte[] data, final Map<String, List<String>> headers) {
    this.data = data;
    this.headers = headers;
  }
  
  public byte[] getData() {
    return data;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }
}
