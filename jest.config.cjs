module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  extensionsToTreatAsEsm: ['.ts'], // Treat .ts files as ESM
  moduleFileExtensions: ['ts', 'tsx', 'js', 'mjs', 'json', 'node'], // Add 'mjs'
  transform: {
    '^.+\\.tsx?$': [
      'ts-jest',
      {
        useESM: true,
        tsconfig: 'tsconfig.json', // Explicitly point to tsconfig
      },
    ],
  },
  // No moduleNameMapper or transformIgnorePatterns initially.
  // Rely on Node's resolver and ts-jest's ESM transformation.
};
