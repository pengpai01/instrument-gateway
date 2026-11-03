package com.danah.instrument.gateway.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Protects POST /api/v1/ingest with Authorization: Bearer &lt;key&gt; or X-Api-Key.
 */
@Component
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    public static final String HEADER_API_KEY = "X-Api-Key";
    private static final String INGEST_PATH = "/api/v1/ingest";

    private final ApiKeyAuthenticator authenticator;

    public ApiKeyAuthFilter(ApiKeyAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !(request.getMethod().equalsIgnoreCase("POST")
                && INGEST_PATH.equals(request.getRequestURI()));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String key = extractKey(request);
        if (!authenticator.matches(key)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().write("{\"error\":\"unauthorized\",\"message\":\"missing or invalid API key\"}");
            return;
        }
        filterChain.doFilter(request, response);
    }

    static String extractKey(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return header.substring(7).trim();
        }
        String apiKey = request.getHeader(HEADER_API_KEY);
        return apiKey != null ? apiKey.trim() : null;
    }
}
