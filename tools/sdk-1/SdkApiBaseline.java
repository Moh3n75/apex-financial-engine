import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import javax.tools.ToolProvider;

/** Java 21 only; no class loading, dependency resolution, or third-party comparison library. */
public class SdkApiBaseline {
    static final String VERSION = "sdk-1/v1";
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[1]).toAbsolutePath();
        if (args[0].equals("self-test")) { selfTest(root); return; }
        Path baseline = root.resolve("tools/sdk-1/baseline/public-api-v1.txt");
        var snapshot = new StringBuilder("# Java 21 public/protected declarations; conservative exact compatibility gate v1\n");
        var inventory = new StringBuilder("module\tbinaryName\tclassification\tjvmPublic\n");
        var modules = Files.list(root).filter(p -> p.getFileName().toString().startsWith("apex-sdk-")
                && Files.exists(p.resolve("pom.xml"))).sorted().toList();
        var frozenNames = new TreeMap<String, Set<String>>();
        for (String line : Files.readAllLines(root.resolve("tools/sdk-1/baseline/api-inventory-v1.tsv")).subList(1,
                Files.readAllLines(root.resolve("tools/sdk-1/baseline/api-inventory-v1.tsv")).size())) {
            String[] fields = line.split("\\t");
            frozenNames.computeIfAbsent(fields[0], key -> new TreeSet<>()).add(fields[1]);
        }
        // Dependency-only v1 role artifacts have no inventory rows.
        frozenNames.putIfAbsent("apex-sdk-grpc-client", Set.of());
        frozenNames.putIfAbsent("apex-sdk-grpc-server", Set.of());
        var expectedModules = new TreeSet<>(frozenNames.keySet());
        expectedModules.add("apex-sdk-messaging-core");
        if (!new TreeSet<>(modules.stream().map(p -> p.getFileName().toString()).toList()).equals(expectedModules))
            throw new IllegalStateException("Unexpected SDK module set; explicit extension review required");
        var extension = new StringBuilder("# AOR-01 v2 additive internal declarations; v1 remains exact\n");
        for (Path module: modules) {
            String name=module.getFileName().toString(); Path classes=module.resolve("target/classes");
            var names=new ArrayList<String>();
            if (Files.exists(classes)) try (var paths=Files.walk(classes)) {
                for (Path file: paths.filter(p -> p.toString().endsWith(".class")).sorted().toList()) {
                    String binary=classes.relativize(file).toString().replace('\\','.').replace('/','.').replaceAll("\\.class$","");
                    // Keep package-private hierarchy support and bridge methods too; ignore anonymous bodies.
                    if (binary.matches(".*\\$[0-9].*")) continue;
                    boolean visible=publicFlag(file);
                    names.add(binary);
                    inventory.append(name).append('\t').append(binary).append('\t').append(category(name,binary))
                            .append('\t').append(visible).append('\n');
                }
            }
            if(names.isEmpty() && Files.exists(module.resolve("src/main/java")))
                throw new IllegalStateException("Compile module before checking API: "+name);
            Set<String> oldNames = frozenNames.getOrDefault(name, Set.of());
            if (!names.containsAll(oldNames)) throw new IllegalStateException("Missing frozen classes in " + name);
            if (frozenNames.containsKey(name)) {
                snapshot.append("\nMODULE ").append(name).append('\n');
                if (!oldNames.isEmpty()) snapshot.append(declarations(classes, names.stream().filter(oldNames::contains).toList()));
            }
            var added = names.stream().filter(n -> !oldNames.contains(n)).toList();
            if (!added.isEmpty()) {
                extension.append("\nMODULE ").append(name).append('\n');
                extension.append(declarations(classes, added));
            }
        }
        Path extensionOutput = root.resolve("out/sdk-1/api-extension-current.txt");
        Files.createDirectories(extensionOutput.getParent());
        Files.writeString(extensionOutput, extension.toString());
        Path extensionBaseline = root.resolve("tools/sdk-1/baseline/public-api-aor-01-v2.txt");
        if (!Files.exists(extensionBaseline) || !Files.readString(extensionBaseline).replace("\r\n", "\n").equals(extension.toString()))
            throw new IllegalStateException("Additive declaration extension changed; review " + extensionOutput);
        String actual=snapshot.toString();
        Path output=root.resolve("out/sdk-1/api-current.txt"); Files.createDirectories(output.getParent());
        Files.writeString(output,actual); Files.writeString(root.resolve("out/sdk-1/api-inventory-current.tsv"),inventory);
        if(args[0].equals("capture")) {
            if(Files.exists(baseline)) throw new IllegalStateException("Baseline exists; never silently regenerate a reviewed version");
            Files.createDirectories(baseline.getParent()); Files.writeString(baseline,actual);
            Files.writeString(baseline.resolveSibling("api-inventory-v1.tsv"),inventory);
        } else if(args[0].equals("check")) {
            String expected=Files.readString(baseline).replace("\r\n","\n");
            if(!expected.equals(actual)) {
                var old=expected.lines().toList();var next=actual.lines().toList();int i=0;
                while(i<Math.min(old.size(),next.size()) && old.get(i).equals(next.get(i)))i++;
                throw new IllegalStateException("API compatibility baseline changed at line "+(i+1)+
                    "; review out/sdk-1/api-current.txt against "+baseline+"; migration/review required");
            }
        } else throw new IllegalArgumentException("capture|check|self-test ROOT");
        System.out.println("API "+args[0]+" PASS: 13 modules, inventory="+inventory.toString().lines().count());
    }
    static String category(String module,String name) {
        if(name.contains(".internal.wire."))return "generated-wire-internal";
        if(name.contains(".internal."))return "internal-linkage-covered";
        if(name.endsWith("AutoConfiguration") || name.contains("AutoConfiguration$"))return "autoconfiguration";
        if(name.endsWith("Properties") || name.contains("Properties$"))return "configuration";
        if(module.equals("apex-sdk-api") || name.contains(".api.") || name.contains(".security."))return "supported-api-spi";
        return "public-adapter-or-hierarchy-support";
    }
    static boolean publicFlag(Path file) throws Exception {
        try(var in=new DataInputStream(Files.newInputStream(file))) {
            if(in.readInt()!=0xcafebabe)throw new IOException("Not a class file");in.readUnsignedShort();in.readUnsignedShort();
            int count=in.readUnsignedShort();
            for(int i=1;i<count;i++)switch(in.readUnsignedByte()) {
                case 1 -> in.readUTF();
                case 3,4 -> in.skipNBytes(4);
                case 5,6 -> {in.skipNBytes(8);i++;}
                case 7,8,16,19,20 -> in.skipNBytes(2);
                case 9,10,11,12,17,18 -> in.skipNBytes(4);
                case 15 -> in.skipNBytes(3);
                default -> throw new IOException("Unsupported constant pool tag");
            }
            return (in.readUnsignedShort()&1)!=0;
        }
    }
    static String declarations(Path classes,List<String> names) throws Exception {
        var command=new ArrayList<String>(List.of(Path.of(System.getProperty("java.home"),"bin","javap").toString(),
                "-J-Duser.language=en","-protected","-s","-constants","-classpath",classes.toString()));
        command.addAll(names);
        var process=new ProcessBuilder(command).redirectErrorStream(true).start();
        String result=new String(process.getInputStream().readAllBytes(),StandardCharsets.UTF_8);
        if(process.waitFor()!=0)throw new IOException(result);
        return result.lines().filter(s -> !s.startsWith("Compiled from ")).map(String::stripTrailing)
                .collect(java.util.stream.Collectors.joining("\n","","\n"));
    }
    static void selfTest(Path root) throws Exception {
        Path dir=root.resolve("out/sdk-1/api-self-test");Files.createDirectories(dir);
        String prefix="public class Fixture { public static final int C=1; protected String hook(){return \"x\";} ";
        String old=prefix+"public String read(String value){return value;} }";
        String expected=compile(dir.resolve("old"),old);
        if(!expected.equals(compile(dir.resolve("body"),old.replace("return value;","return \"changed\";"))))throw new AssertionError("Bodies are not API");
        var broken=List.of(old.replace("String read(String","String read(int"),
                old.replace("public String read","public static String read"),old.replace("public class","public final class"),
                old.replace("protected String hook","private String hook"),old.replace("C=1","C=2"),
                prefix+"public int read(String value){return 1;} }",prefix+"}",
                old.replace("public class Fixture","public class Fixture extends java.util.ArrayList<String>"));
        for(int i=0;i<broken.size();i++)if(expected.equals(compile(dir.resolve("broken-"+i),broken.get(i))))
            throw new AssertionError("Missed incompatibility "+i);
        System.out.println("API self-test PASS: body-only accepted; 8 incompatible/contract mutations rejected");
    }
    static String compile(Path dir,String source) throws Exception {
        Files.createDirectories(dir);Path java=dir.resolve("Fixture.java");Files.writeString(java,source);
        // The parameter mutation retains a valid body so failure must come from API comparison.
        if(source.contains("read(int value)"))Files.writeString(java,source.replace("return value;","return Integer.toString(value);"));
        if(ToolProvider.getSystemJavaCompiler().run(null,null,null,"--release","21","-d",dir.toString(),java.toString())!=0)
            throw new AssertionError("Mutation fixture did not compile");
        return declarations(dir,List.of("Fixture"));
    }
}
