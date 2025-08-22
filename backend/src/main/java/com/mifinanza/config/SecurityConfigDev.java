package com.mifinanza.config;

import org.springframework.context.annotation.*;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@Profile("dev")
public class SecurityConfigDev {

  @Bean
  SecurityFilterChain api(HttpSecurity http) throws Exception {
    http
      // desactivo CSRF solo para /api/** porque usamos fetch/XHR desde el front
      .csrf(csrf -> csrf.ignoringRequestMatchers("/api/**"))
      .authorizeHttpRequests(auth -> auth
        .requestMatchers("/api/**").permitAll() // en DEV dejamos libre la API
        .anyRequest().permitAll()
      )
      .httpBasic(Customizer.withDefaults()); // útil para probar rápido
    return http.build();
  }
}
