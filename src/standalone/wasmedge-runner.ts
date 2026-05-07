import { existsSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export interface FlatSQLWasmEdgeRunnerBuildOptions {
  outputPath: string;
  sourcePath?: string;
  includeDir?: string;
  libDir?: string;
  cxx?: string;
  extraArgs?: string[];
}

export interface FlatSQLWasmEdgeRunnerArtifact {
  outputPath: string;
  sourcePath: string;
  includeDir: string;
  libDir: string;
  command: string;
  args: string[];
}

function defaultSourcePath(): string {
  return fileURLToPath(new URL('../../wasm/native/flatsql_wasmedge_runner.cpp', import.meta.url));
}

function firstExisting(candidates: Array<string | undefined>): string | undefined {
  return candidates.find((candidate) => candidate !== undefined && existsSync(candidate));
}

function defaultIncludeDir(): string | undefined {
  return firstExisting([
    process.env.WASMEDGE_INCLUDE_DIR,
    join(homedir(), '.wasmedge', 'include'),
    '/opt/homebrew/include',
    '/usr/local/include',
  ]);
}

function defaultLibDir(): string | undefined {
  return firstExisting([
    process.env.WASMEDGE_LIB_DIR,
    join(homedir(), '.wasmedge', 'lib'),
    '/opt/homebrew/lib',
    '/usr/local/lib',
  ]);
}

function resolveInputs(options: Partial<FlatSQLWasmEdgeRunnerBuildOptions> = {}): Omit<
  Required<FlatSQLWasmEdgeRunnerBuildOptions>,
  'extraArgs'
> & { extraArgs: string[] } {
  const sourcePath = options.sourcePath ?? defaultSourcePath();
  const includeDir = options.includeDir ?? defaultIncludeDir();
  const libDir = options.libDir ?? defaultLibDir();

  if (!includeDir) {
    throw new Error('WasmEdge include directory not found. Set WASMEDGE_INCLUDE_DIR.');
  }
  if (!libDir) {
    throw new Error('WasmEdge library directory not found. Set WASMEDGE_LIB_DIR.');
  }

  return {
    outputPath: options.outputPath ?? 'flatsql-wasmedge-runner',
    sourcePath,
    includeDir,
    libDir,
    cxx: options.cxx ?? process.env.CXX ?? 'c++',
    extraArgs: options.extraArgs ?? [],
  };
}

export function hasWasmEdgeBuildInputs(options: Partial<FlatSQLWasmEdgeRunnerBuildOptions> = {}): boolean {
  try {
    const inputs = resolveInputs({ ...options, outputPath: options.outputPath ?? 'flatsql-wasmedge-runner' });
    return (
      existsSync(inputs.sourcePath) &&
      existsSync(join(inputs.includeDir, 'wasmedge', 'wasmedge.h')) &&
      (existsSync(join(inputs.libDir, 'libwasmedge.dylib')) ||
        existsSync(join(inputs.libDir, 'libwasmedge.so')) ||
        existsSync(join(inputs.libDir, 'libwasmedge.a'))) &&
      spawnSync(inputs.cxx, ['--version'], { encoding: 'utf8' }).status === 0
    );
  } catch {
    return false;
  }
}

export async function buildFlatSQLWasmEdgeRunner(
  options: FlatSQLWasmEdgeRunnerBuildOptions
): Promise<FlatSQLWasmEdgeRunnerArtifact> {
  const inputs = resolveInputs(options);
  await mkdir(dirname(inputs.outputPath), { recursive: true });

  const args = [
    inputs.sourcePath,
    '-std=c++17',
    '-O2',
    '-I',
    inputs.includeDir,
    '-L',
    inputs.libDir,
    '-lwasmedge',
    '-o',
    inputs.outputPath,
    ...inputs.extraArgs,
  ];

  if (process.platform === 'darwin') {
    args.splice(args.length - inputs.extraArgs.length, 0, '-Wl,-rpath,' + inputs.libDir);
  }

  const result = spawnSync(inputs.cxx, args, {
    encoding: 'utf8',
    env: {
      ...process.env,
      DYLD_LIBRARY_PATH: process.env.DYLD_LIBRARY_PATH ?? inputs.libDir,
      LD_LIBRARY_PATH: process.env.LD_LIBRARY_PATH ?? inputs.libDir,
    },
  });

  if (result.status !== 0) {
    throw new Error(
      [
        `Failed to build FlatSQL WasmEdge runner with ${inputs.cxx}.`,
        `Command: ${inputs.cxx} ${args.join(' ')}`,
        result.stdout,
        result.stderr,
      ]
        .filter(Boolean)
        .join('\n')
    );
  }

  return {
    outputPath: inputs.outputPath,
    sourcePath: inputs.sourcePath,
    includeDir: inputs.includeDir,
    libDir: inputs.libDir,
    command: inputs.cxx,
    args,
  };
}
