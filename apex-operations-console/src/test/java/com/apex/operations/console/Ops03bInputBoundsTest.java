package com.apex.operations.console;
import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;import org.springframework.mock.web.*;
class Ops03bInputBoundsTest {
 @Test void chunkedUnknownLengthBodyIsDeniedWithoutReadingOrEchoingIt()throws Exception {
  var request=new MockHttpServletRequest("GET","/ops/v1/overview") { @Override public long getContentLengthLong(){return -1;} };
  request.addHeader("Transfer-Encoding","chunked");request.setContent("CANARY_BODY_TOKEN".getBytes(java.nio.charset.StandardCharsets.UTF_8));
  var response=new MockHttpServletResponse();
  new ReadOnlyFilter().doFilter(request,response,(a,b)->{throw new AssertionError("Body reached read API");});
  assertThat(response.getStatus()).isEqualTo(400);assertThat(response.getContentAsString()).doesNotContain("CANARY_BODY_TOKEN");
 }
 @Test void declaredBodyIsDeniedButBodylessGetStillWorks()throws Exception {
  var request=new MockHttpServletRequest("GET","/ops/v1/cells");request.setContent(new byte[]{1});var response=new MockHttpServletResponse();
  new ReadOnlyFilter().doFilter(request,response,(a,b)->{throw new AssertionError();});assertThat(response.getStatus()).isEqualTo(400);
  var empty=new MockHttpServletRequest("GET","/ops/v1/overview");var ok=new MockHttpServletResponse();var reached=new java.util.concurrent.atomic.AtomicBoolean();
  new ReadOnlyFilter().doFilter(empty,ok,(a,b)->reached.set(true));assertThat(reached).isTrue();assertThat(ok.getHeader("Cache-Control")).isEqualTo("no-store");
 }
}
