# Python API Reference

Auto-generated documentation from source code docstrings.

## Service Layer

### ModelStore

::: orionbelt.service.model_store.ModelStore
    options:
      show_source: true
      members:
        - load_model
        - get_model
        - describe
        - list_models
        - remove_model
        - compile_query
        - validate

### SessionManager

::: orionbelt.service.session_manager.SessionManager
    options:
      show_source: true
      members:
        - start
        - stop
        - create_session
        - get_store
        - get_session
        - close_session
        - list_sessions
        - active_count
        - get_or_create_default

### SessionInfo

::: orionbelt.service.session_manager.SessionInfo
    options:
      show_source: true

## Compiler Pipeline

::: orionbelt.compiler.pipeline.CompilationPipeline
    options:
      show_source: true
      members:
        - compile

## Query Resolution

::: orionbelt.compiler.resolution.QueryResolver
    options:
      show_source: true
      members:
        - resolve

## Star Schema Planner

::: orionbelt.compiler.star.StarSchemaPlanner
    options:
      show_source: true
      members:
        - plan

## CFL Planner

::: orionbelt.compiler.cfl.CFLPlanner
    options:
      show_source: true
      members:
        - plan

## Join Graph

::: orionbelt.compiler.graph.JoinGraph
    options:
      show_source: true
      members:
        - find_join_path
        - build_join_condition
        - detect_cycles

## Code Generator

::: orionbelt.compiler.codegen.CodeGenerator
    options:
      show_source: true
      members:
        - generate

## Dialect Base

::: orionbelt.dialect.base.Dialect
    options:
      show_source: true

::: orionbelt.dialect.base.DialectCapabilities
    options:
      show_source: true

## Dialect Registry

::: orionbelt.dialect.registry.DialectRegistry
    options:
      show_source: true
      members:
        - get
        - available
        - register

## YAML Parser

::: orionbelt.parser.loader.TrackedLoader
    options:
      show_source: true
      members:
        - load
        - load_string
        - load_model_directory

## Reference Resolver

::: orionbelt.parser.resolver.ReferenceResolver
    options:
      show_source: true
      members:
        - resolve

## Semantic Validator

::: orionbelt.parser.validator.SemanticValidator
    options:
      show_source: true
      members:
        - validate

## Semantic Model

::: orionbelt.models.semantic.SemanticModel
    options:
      show_source: true

::: orionbelt.models.semantic.DataObject
    options:
      show_source: true

::: orionbelt.models.semantic.Dimension
    options:
      show_source: true

::: orionbelt.models.semantic.Measure
    options:
      show_source: true

::: orionbelt.models.semantic.Metric
    options:
      show_source: true

## Query Models

::: orionbelt.models.query.QueryObject
    options:
      show_source: true

::: orionbelt.models.query.QuerySelect
    options:
      show_source: true

::: orionbelt.models.query.QueryFilter
    options:
      show_source: true

::: orionbelt.models.query.UsePathName
    options:
      show_source: true

::: orionbelt.models.query.DimensionRef
    options:
      show_source: true

## Error Models

::: orionbelt.models.errors.SemanticError
    options:
      show_source: true

::: orionbelt.models.errors.ValidationResult
    options:
      show_source: true

::: orionbelt.models.errors.SourceSpan
    options:
      show_source: true

## SQL AST Nodes

::: orionbelt.ast.nodes.Select
    options:
      show_source: true

::: orionbelt.ast.nodes.ColumnRef
    options:
      show_source: true

::: orionbelt.ast.nodes.FunctionCall
    options:
      show_source: true

::: orionbelt.ast.nodes.BinaryOp
    options:
      show_source: true

::: orionbelt.ast.nodes.Literal
    options:
      show_source: true

## AST Builder

::: orionbelt.ast.builder.QueryBuilder
    options:
      show_source: true

## API Schemas

::: orionbelt.api.schemas
    options:
      show_source: true
      members:
        - SessionCreateRequest
        - SessionResponse
        - SessionListResponse
        - ModelLoadRequest
        - ModelLoadResponse
        - ModelSummaryResponse
        - SessionQueryRequest
        - QueryCompileResponse
        - ValidateRequest
        - ValidateResponse
        - DialectListResponse
        - HealthResponse

## Settings

::: orionbelt.settings.Settings
    options:
      show_source: true
