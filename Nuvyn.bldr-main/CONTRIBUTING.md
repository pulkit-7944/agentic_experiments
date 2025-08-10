# Contributing to Nuvyn.bldr

Thank you for your interest in contributing to Nuvyn.bldr! This document provides guidelines for contributing to the project.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct.

## How Can I Contribute?

### Reporting Bugs
- Use the issue template for bug reports
- Include detailed steps to reproduce the bug
- Provide system information and error messages
- Include screenshots if applicable

### Suggesting Enhancements
- Use the feature request template
- Clearly describe the enhancement
- Explain why this enhancement would be useful
- Provide examples if possible

### Pull Requests
1. Fork the repository
2. Create a feature branch from `develop`
3. Make your changes
4. Write or update tests
5. Ensure all tests pass
6. Update documentation if needed
7. Submit a pull request

## Development Setup

### Prerequisites
- Git 2.50.1 or higher
- Node.js (if applicable)
- Any other project-specific requirements

### Local Development
1. Fork and clone the repository
2. Install dependencies (if applicable)
3. Set up your development environment
4. Run tests to ensure everything works

## Branch Naming Convention

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring
- `test/description` - Adding or updating tests
- `chore/description` - Maintenance tasks

## Commit Message Format

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types
- `feat` - A new feature
- `fix` - A bug fix
- `docs` - Documentation only changes
- `style` - Changes that do not affect the meaning of the code
- `refactor` - A code change that neither fixes a bug nor adds a feature
- `perf` - A code change that improves performance
- `test` - Adding missing tests or correcting existing tests
- `chore` - Changes to the build process or auxiliary tools

### Examples
```
feat: add user authentication system
fix(auth): resolve login validation issue
docs: update API documentation
style: format code according to style guide
```

## Pull Request Process

1. **Branch**: Create a feature branch from `develop`
2. **Changes**: Make your changes following the coding standards
3. **Tests**: Ensure all tests pass and add new tests if needed
4. **Documentation**: Update documentation if your changes require it
5. **Review**: Request reviews from maintainers
6. **Merge**: Once approved, your PR will be merged

## Code Review Guidelines

### For Contributors
- Respond to review comments promptly
- Make requested changes or explain why they're not needed
- Keep commits focused and logical

### For Reviewers
- Be constructive and respectful
- Focus on the code, not the person
- Provide specific, actionable feedback
- Approve when satisfied with the changes

## Testing

- Write tests for new features
- Ensure existing tests still pass
- Aim for good test coverage
- Include integration tests for complex features

## Documentation

- Update README.md if needed
- Add inline comments for complex code
- Update API documentation if applicable
- Include examples for new features

## Release Process

1. Features are merged to `develop`
2. Release branches are created from `develop`
3. Testing and bug fixes happen on release branches
4. Release branches are merged to `main` and tagged
5. Release branches are merged back to `develop`

## Questions or Need Help?

- Open an issue for questions
- Join our community discussions
- Contact maintainers directly

Thank you for contributing to Nuvyn.bldr! 