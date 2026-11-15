package com.apex.compat;

import com.apex.platform.events.EventMetadata;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PublicContractShapeTest {
    @Test void allApiRecordsEnumsAndRuntimeAnnotationsKeepTheirV1Shape() throws Exception {
        Path classes=Path.of(EventMetadata.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        // Maven test uses classes; verify can already have packaged reactor prerequisites.
        var names=new ArrayList<String>();
        if(Files.isDirectory(classes))try(var files=Files.walk(classes)) {
            for(Path file:files.filter(p->p.toString().endsWith(".class")).toList())
                names.add(classes.relativize(file).toString().replace('\\','.').replace('/','.').replaceAll("\\.class$",""));
        } else try(var jar=new java.util.jar.JarFile(classes.toFile())) {
            names.addAll(jar.stream().map(java.util.jar.JarEntry::getName).filter(n->n.endsWith(".class"))
                    .map(n->n.replace('/','.').replaceAll("\\.class$","")).toList());
        }
        Collections.sort(names);var snapshot=new StringBuilder();int count=0;
        for(String name:names) {
            Class<?> type=Class.forName(name,false,getClass().getClassLoader());
            if(!Modifier.isPublic(type.getModifiers()))continue;
            if(!type.isRecord() && !type.isEnum() && !type.isAnnotation())continue;
            count++;snapshot.append(name).append('\n');annotations(type.getAnnotations(),snapshot);
            if(type.isEnum())snapshot.append("enum ").append(Arrays.toString(type.getEnumConstants())).append('\n');
            if(type.isRecord())for(var component:type.getRecordComponents()) {
                snapshot.append("component ").append(component.getName()).append(' ').append(component.getGenericType().getTypeName()).append('\n');
                annotations(component.getAnnotations(),snapshot);annotations(component.getAccessor().getAnnotations(),snapshot);
                annotations(type.getDeclaredField(component.getName()).getAnnotations(),snapshot);
            }
            if(type.isAnnotation())for(var method:Arrays.stream(type.getDeclaredMethods()).sorted(Comparator.comparing(Method::getName)).toList())
                snapshot.append("default ").append(method.getName()).append('=').append(value(method.getDefaultValue())).append('\n');
        }
        assertThat(count).isGreaterThan(40);
        Path frozen=Path.of("src/test/resources/sdk-1/v1/public-contract-shapes.txt");
        if(Boolean.getBoolean("sdk.compat.capture")) {
            if(Files.exists(frozen))throw new IllegalStateException("Refusing to overwrite reviewed contract fixtures");
            Files.writeString(frozen,snapshot);return;
        }
        assertThat(snapshot.toString()).isEqualTo(new String(SdkContractCompatibilityTest.bytes("public-contract-shapes.txt"),java.nio.charset.StandardCharsets.UTF_8).replace("\r\n","\n"));
    }
    private void annotations(Annotation[] annotations,StringBuilder text) throws Exception {
        for(var annotation:Arrays.stream(annotations).sorted(Comparator.comparing(a->a.annotationType().getName())).toList()) {
            text.append("annotation ").append(annotation.annotationType().getName());
            for(var method:Arrays.stream(annotation.annotationType().getDeclaredMethods()).sorted(Comparator.comparing(Method::getName)).toList())
                text.append(' ').append(method.getName()).append('=').append(value(method.invoke(annotation)));
            text.append('\n');
        }
    }
    private String value(Object object) {
        if(object==null)return "<required>";
        if(object instanceof Class<?> type)return type.getName();
        if(object.getClass().isArray()) {
            var values=new ArrayList<String>();for(int i=0;i<Array.getLength(object);i++)values.add(value(Array.get(object,i)));
            return values.toString();
        }
        return object.toString();
    }
}
