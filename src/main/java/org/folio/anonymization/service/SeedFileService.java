package org.folio.anonymization.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@AllArgsConstructor
public class SeedFileService {

  // cannot access our resources via the default resolver when running on a different thread,
  // so we have to create our own with an explicit classloader
  private static final ResourcePatternResolver RESOLVER = new PathMatchingResourcePatternResolver(
    new DefaultResourceLoader(SeedFileService.class.getClassLoader())
  );

  public Stream<Resource> findSeedFiles(String locationPattern) {
    try {
      return Arrays.stream(RESOLVER.getResources("classpath*:seed/" + locationPattern));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to look for resources", e);
    }
  }

  public List<byte[]> getSeedFilesAsBytes(String locationPattern) {
    return findSeedFiles(locationPattern)
      .map(resource -> {
        try {
          return resource.getContentAsByteArray();
        } catch (Exception e) {
          throw new RuntimeException("Failed to read seed file: " + resource.getFilename(), e);
        }
      })
      .toList();
  }

  public List<String> getSeedFilesAsStrings(String locationPattern) {
    return findSeedFiles(locationPattern)
      .map(resource -> {
        try {
          return resource.getContentAsString(Charset.forName("UTF-8"));
        } catch (Exception e) {
          throw new RuntimeException("Failed to read seed file: " + resource.getFilename(), e);
        }
      })
      .toList();
  }

  public String getSeedFileAsString(String location) {
    return getSeedFilesAsStrings(location)
      .stream()
      .findFirst()
      .orElseThrow(() -> new RuntimeException("No seed file found at location: " + location));
  }

  public InputStream getSeedFileAsInputStream(String location) {
    try {
      return findSeedFiles(location)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No seed file found at location: " + location))
        .getInputStream();
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to look for resources", e);
    }
  }
}
