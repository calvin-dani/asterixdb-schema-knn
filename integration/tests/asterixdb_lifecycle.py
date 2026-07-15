"""AsterixDB process lifecycle management (start/stop/restart)."""

import os
import signal
import subprocess
import threading
import time
import xml.etree.ElementTree as ET


class AsterixDBLifecycleManager:
    """Manages the AsterixDB in-process cluster lifecycle.

    Starts AsterixHyracksIntegrationUtil via JVM, with support for
    cleanup.start toggling (true on first start, false on restart to
    preserve flushed data).

    Classpath construction mirrors IntelliJ's approach:
      1. All project modules' target/classes + target/test-classes
      2. src/test/resources for asterix-app
      3. Third-party dependency jars from ~/.m2/repository
    """

    def __init__(self, config, config_dir):
        self.config = config
        self.config_dir = config_dir
        self.project_root = os.path.normpath(
            os.path.join(config_dir, config["project_root"])
        )
        self.conf_file = os.path.join(self.project_root, config["conf_file"])
        self.main_class = config["main_class"]
        self.jvm_args = config.get(
            "jvm_args",
            '-Xms128m -Xmx128m '
            '-Dnode.Resolver='
            '"org.apache.asterix.external.util.IdentitiyResolverFactory"'
        )
        self.startup_timeout = config.get("startup_timeout", 120)
        self.shutdown_timeout = config.get("shutdown_timeout", 30)
        self.process = None
        self._classpath = None
        self._output_thread = None

    def _build_classpath(self):
        """Build classpath using multiple strategies with fallback."""
        if self._classpath:
            return self._classpath

        app_dir = os.path.join(self.project_root, "asterixdb", "asterix-app")
        cache_file = os.path.join(app_dir, "target", "test-classpath.txt")

        deps_cp = None

        # Strategy 1: Use cached classpath file
        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                deps_cp = f.read().strip()
            if deps_cp:
                print("[lifecycle] Using cached classpath from "
                      "target/test-classpath.txt")

        # Strategy 2: Try Maven dependency:build-classpath
        if not deps_cp:
            try:
                print("[lifecycle] Building classpath via Maven...")
                result = subprocess.run(
                    ["mvn", "-q", "dependency:build-classpath",
                     "-Dmdep.outputFile=target/test-classpath.txt",
                     "-DincludeScope=test"],
                    cwd=app_dir, capture_output=True, text=True, timeout=120
                )
                if result.returncode == 0 and os.path.exists(cache_file):
                    with open(cache_file, "r") as f:
                        deps_cp = f.read().strip()
                    print("[lifecycle] Maven classpath built successfully")
                else:
                    print(f"[lifecycle] Maven classpath failed, "
                          f"using disk-based fallback...")
            except Exception as e:
                print(f"[lifecycle] Maven classpath error: {e}")
                print("[lifecycle] Using disk-based fallback...")

        # Strategy 3: Construct from disk (like IntelliJ does)
        if not deps_cp:
            deps_cp = self._build_classpath_from_disk()
            # Cache for future runs
            try:
                with open(cache_file, "w") as f:
                    f.write(deps_cp)
                print("[lifecycle] Classpath cached to "
                      "target/test-classpath.txt")
            except OSError:
                pass

        # Combine: all module target dirs + test resources + dependency jars
        # The deps_cp from disk fallback already includes module target dirs,
        # but Maven's output does not, so we always prepend them.
        parts = []

        # Add all module target/classes and target/test-classes
        module_dirs = self._find_module_target_dirs()
        parts.extend(module_dirs)

        # Add test resources for asterix-app
        test_res = os.path.join(app_dir, "src", "test", "resources")
        if os.path.isdir(test_res):
            parts.append(test_res)

        # Add dependency jars, but filter out project SNAPSHOT jars
        # that are already covered by target/classes dirs above.
        # Having both causes "Duplicate format" errors from service loaders.
        project_groups = ("org/apache/asterix/", "org/apache/hyracks/")
        for jar in deps_cp.split(os.pathsep):
            if jar and not any(g in jar for g in project_groups):
                parts.append(jar)

        self._classpath = os.pathsep.join(parts)
        return self._classpath

    def _find_module_target_dirs(self):
        """Find all target/classes and target/test-classes in the project."""
        dirs = []
        for root, subdirs, files in os.walk(self.project_root):
            basename = os.path.basename(root)
            parent = os.path.basename(os.path.dirname(root))
            if parent == "target" and basename in ("classes", "test-classes"):
                dirs.append(root)
            # Prune directories to speed up walking
            subdirs[:] = [
                d for d in subdirs
                if d not in ('.git', '.idea', 'node_modules', 'integration',
                             'doc', 'src', '.claude')
            ]
        return sorted(dirs)

    def _build_classpath_from_disk(self):
        """Construct dependency classpath by scanning .m2 repository.

        Parses asterix-app/pom.xml and parent POMs to find third-party
        dependency coordinates, then resolves jar paths in ~/.m2/repository.
        Also includes project SNAPSHOT jars for cross-module deps.
        """
        m2_repo = os.path.expanduser("~/.m2/repository")
        jar_paths = set()

        # 1. Add all project SNAPSHOT jars from .m2/repository
        for group in ("org/apache/asterix", "org/apache/hyracks"):
            group_dir = os.path.join(m2_repo, group)
            if not os.path.isdir(group_dir):
                continue
            for root, dirs, files in os.walk(group_dir):
                for f in files:
                    if (f.endswith(".jar")
                            and "SNAPSHOT" in f
                            and not f.endswith("-sources.jar")
                            and not f.endswith("-javadoc.jar")):
                        jar_paths.add(os.path.join(root, f))

        # 2. Parse POM hierarchy to find third-party deps
        third_party = self._collect_third_party_deps()
        for group_id, artifact_id, version in third_party:
            jar_name = f"{artifact_id}-{version}.jar"
            jar_path = os.path.join(
                m2_repo,
                group_id.replace(".", "/"),
                artifact_id,
                version,
                jar_name,
            )
            if os.path.exists(jar_path):
                jar_paths.add(jar_path)

        print(f"[lifecycle] Disk-based classpath: {len(jar_paths)} jars")
        return os.pathsep.join(sorted(jar_paths))

    def _collect_third_party_deps(self):
        """Collect third-party dependency coordinates from POM files.

        Walks up the POM hierarchy from asterix-app to resolve dependencies
        and properties. Returns list of (groupId, artifactId, version) tuples
        for non-project dependencies.
        """
        deps = set()
        properties = {}
        project_groups = {"org.apache.asterix", "org.apache.hyracks"}

        # Collect POM files: asterix-app → parent chain
        pom_files = self._find_pom_chain()

        ns = {"m": "http://maven.apache.org/POM/4.0.0"}

        # First pass: collect all properties
        for pom_path in pom_files:
            try:
                tree = ET.parse(pom_path)
                root = tree.getroot()
                props_el = root.find("m:properties", ns)
                if props_el is not None:
                    for prop in props_el:
                        # Strip namespace from tag name
                        tag = prop.tag
                        if "}" in tag:
                            tag = tag.split("}", 1)[1]
                        if prop.text:
                            properties[tag] = prop.text.strip()

                # Also get project version
                ver_el = root.find("m:version", ns)
                if ver_el is not None and ver_el.text:
                    properties["project.version"] = ver_el.text.strip()
            except ET.ParseError:
                continue

        # Second pass: collect dependencies and dependencyManagement
        dep_versions = {}  # (groupId, artifactId) -> version from mgmt
        for pom_path in pom_files:
            try:
                tree = ET.parse(pom_path)
                root = tree.getroot()

                # Dependency management (version defaults)
                for dep in root.findall(
                    ".//m:dependencyManagement/m:dependencies/m:dependency",
                    ns
                ):
                    g, a, v = self._parse_dep_element(dep, ns)
                    if g and a and v:
                        v = self._resolve_property(v, properties)
                        dep_versions[(g, a)] = v

                # Direct dependencies
                # Only look at top-level <dependencies>, not nested ones
                deps_el = root.find("m:dependencies", ns)
                if deps_el is not None:
                    for dep in deps_el.findall("m:dependency", ns):
                        g, a, v = self._parse_dep_element(dep, ns)
                        if not g or not a:
                            continue
                        if g in project_groups:
                            continue  # skip intra-project deps

                        # Resolve version
                        if v:
                            v = self._resolve_property(v, properties)
                        elif (g, a) in dep_versions:
                            v = dep_versions[(g, a)]
                        else:
                            continue  # can't resolve version

                        if v:
                            deps.add((g, a, v))
            except ET.ParseError:
                continue

        return deps

    def _parse_dep_element(self, dep_el, ns):
        """Extract groupId, artifactId, version from a <dependency> element."""
        g_el = dep_el.find("m:groupId", ns)
        a_el = dep_el.find("m:artifactId", ns)
        v_el = dep_el.find("m:version", ns)
        g = g_el.text.strip() if g_el is not None and g_el.text else None
        a = a_el.text.strip() if a_el is not None and a_el.text else None
        v = v_el.text.strip() if v_el is not None and v_el.text else None
        return g, a, v

    def _resolve_property(self, value, properties):
        """Resolve Maven property references like ${foo.version}."""
        if not value or "${" not in value:
            return value
        import re
        def replacer(m):
            prop_name = m.group(1)
            return properties.get(prop_name, m.group(0))
        # Iterate to handle nested references
        for _ in range(5):
            new_val = re.sub(r'\$\{([^}]+)\}', replacer, value)
            if new_val == value:
                break
            value = new_val
        if "${" in value:
            return None  # unresolved
        return value

    def _find_pom_chain(self):
        """Find POM files from asterix-app up through parent hierarchy."""
        pom_files = []

        # asterix-app pom
        app_pom = os.path.join(
            self.project_root, "asterixdb", "asterix-app", "pom.xml"
        )
        if os.path.exists(app_pom):
            pom_files.append(app_pom)

        # Walk parent chain
        current = app_pom
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        visited = set()
        for _ in range(10):  # max depth
            if current in visited or not os.path.exists(current):
                break
            visited.add(current)
            try:
                tree = ET.parse(current)
                root = tree.getroot()
                parent = root.find("m:parent", ns)
                if parent is None:
                    break
                rel_path = parent.find("m:relativePath", ns)
                if rel_path is not None and rel_path.text:
                    parent_path = os.path.normpath(
                        os.path.join(os.path.dirname(current), rel_path.text)
                    )
                else:
                    parent_path = os.path.normpath(
                        os.path.join(os.path.dirname(current), "..", "pom.xml")
                    )
                # If path is a directory, look for pom.xml inside it
                if os.path.isdir(parent_path):
                    parent_path = os.path.join(parent_path, "pom.xml")
                if os.path.exists(parent_path):
                    pom_files.append(parent_path)
                    current = parent_path
                else:
                    break
            except ET.ParseError:
                break

        return pom_files

    def start(self, cleanup=True):
        """Start the AsterixDB cluster.

        Args:
            cleanup: If True, sets -Dcleanup.start=true to delete old data.
                     If False, preserves existing data (for restart scenarios).
        """
        if self.process and self.process.poll() is None:
            print("[lifecycle] Cluster already running (pid=%d)" %
                  self.process.pid)
            return

        classpath = self._build_classpath()

        jvm_parts = self.jvm_args.split()
        cmd = [
            "java",
            *jvm_parts,
            f"-Dcleanup.start={'true' if cleanup else 'false'}",
            "-Dcleanup.shutdown=false",
            "-cp", classpath,
            self.main_class,
        ]

        print(f"[lifecycle] Starting cluster (cleanup={cleanup})...")
        self.process = subprocess.Popen(
            cmd,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,  # create process group for clean shutdown
        )
        print(f"[lifecycle] Process started (pid={self.process.pid})")

        # Background thread to relay JVM output
        def _relay_output(proc):
            try:
                for line in iter(proc.stdout.readline, b''):
                    text = line.decode('utf-8', errors='replace').rstrip()
                    if text:
                        print(f"[jvm] {text}")
            except (ValueError, OSError):
                pass  # stream closed

        self._output_thread = threading.Thread(
            target=_relay_output, args=(self.process,), daemon=True
        )
        self._output_thread.start()

    def stop(self):
        """Stop the cluster gracefully via SIGINT (triggers JVM shutdown hook).

        The shutdown hook calls deinit() which stops NCs (flushing memory
        components) then CC.
        """
        if not self.process or self.process.poll() is not None:
            print("[lifecycle] No running process to stop")
            return

        pid = self.process.pid
        print(f"[lifecycle] Sending SIGINT to process group (pid={pid})...")

        try:
            os.killpg(os.getpgid(pid), signal.SIGINT)
        except ProcessLookupError:
            print("[lifecycle] Process already terminated")
            return

        # Wait for graceful shutdown
        try:
            self.process.wait(timeout=self.shutdown_timeout)
            print("[lifecycle] Cluster stopped gracefully")
        except subprocess.TimeoutExpired:
            print("[lifecycle] Graceful shutdown timed out, sending SIGKILL...")
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=10)
            print("[lifecycle] Cluster killed")

        self.process = None

    def restart(self):
        """Restart the cluster, preserving flushed data.

        Sends SIGINT to trigger flush via shutdown hook, then restarts
        with cleanup=False.
        """
        print("[lifecycle] Restarting cluster...")
        self.stop()
        time.sleep(3)  # allow OS to release ports
        self.start(cleanup=False)

    def is_running(self):
        """Check if the cluster process is still running."""
        return self.process is not None and self.process.poll() is None
