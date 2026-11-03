package com.apex.operations.console;
import jakarta.servlet.*;import jakarta.servlet.http.*;import java.io.IOException;
import org.springframework.stereotype.Component;
@Component
public final class ReadOnlyFilter implements Filter {
 @Override public void doFilter(ServletRequest req,ServletResponse res,FilterChain chain)throws IOException,ServletException {
 var r=(HttpServletRequest)req;var s=(HttpServletResponse)res;s.setHeader("Cache-Control","no-store");
 if(!r.getMethod().equals("GET")){s.setStatus(405);return;}
 boolean search=java.util.Set.of("/ops/v1/events","/ops/v1/inbox","/ops/v1/requests","/ops/v1/workflows","/ops/v1/recent/events","/ops/v1/recent/inbox","/ops/v1/recent/requests","/ops/v1/recent/workflows").contains(r.getRequestURI());
 if(r.getRequestURI().length()>256||(r.getQueryString()!=null&&r.getQueryString().length()>(search?1536:128))||(r.getContentLengthLong()>0||r.getHeader("Transfer-Encoding")!=null)){s.setStatus(400);return;}
 chain.doFilter(req,res);
 }
}
