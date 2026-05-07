package org.folio.anonymization.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class SeedFileService {

  private final ResourcePatternResolver resourceResolver;

  public Stream<Resource> findSeedFiles(String locationPattern) {
    try {
      return Arrays
        .stream(resourceResolver.getResources("classpath:/seed/" + locationPattern))
        .filter(Resource::isReadable);
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
