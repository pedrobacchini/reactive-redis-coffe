# Instruções Copilot para reactive-redis-coffe

## Visão Geral do Repositório

**Tipo de Projeto**: Aplicação web reativa Spring Boot 2.1.6 com cache Redis  
**Linguagem**: Java 8 (JDK 1.8)  
**Ferramenta de Build**: Maven 3.x  
**Framework**: Spring Boot com Spring WebFlux e Spring Data Redis Reactive  
**Tamanho**: Pequeno (~6 arquivos Java, 25MB com dependências)  
**Classe Principal**: `com.github.pedrobacchini.reactiverediscoffe.ReactiveRedisCoffeApplication`

Este é um projeto de demonstração que apresenta programação reativa com Redis usando Spring Boot. A aplicação fornece endpoints REST para gerenciar dados de café armazenados no Redis, com streams reativos para atualizações em tempo real via Server-Sent Events (SSE).

## Requisitos Críticos de Build

### Requisito de Versão Java (CRÍTICO)
**SEMPRE use Java 8 (JDK 1.8) - o build VAI FALHAR com versões mais recentes do Java.**

O projeto usa Lombok com uma versão antiga que é incompatível com Java 11+. Usar Java 17 (padrão do sistema) causará este erro:
```
Fatal error compiling: java.lang.IllegalAccessError: class lombok.javac.apt.LombokProcessor
cannot access class com.sun.tools.javac.processing.JavacProcessingEnvironment
```

**Comandos obrigatórios antes de QUALQUER operação Maven:**
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Verifique a versão do Java antes de compilar:
```bash
java -version  # Deve mostrar "1.8.0"
```

### Requisito do Redis
- **Compilação**: Funciona sem Redis (use `-DskipTests` para pular testes)
- **Testes**: Requer Redis executando em localhost:6379
- **Execução**: Requer Redis executando em localhost:6379

Para iniciar o Redis:
```bash
sudo service redis-server start
redis-cli ping  # Deve retornar PONG
```

## Comandos de Build (Sequência Validada)

### 1. Limpar o Projeto
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn clean
```
**Tempo**: ~4-5 segundos  
**Redis não é necessário**

### 2. Apenas Compilar (Sem Testes)
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn clean compile
```
**Tempo**: ~10-15 segundos (primeira execução com download de dependências)  
**Redis não é necessário**  
**Indicador de sucesso**: `BUILD SUCCESS` com 5 arquivos fonte compilados

### 3. Executar Testes (Requer Redis)
```bash
# Certifique-se de que o Redis está executando primeiro
sudo service redis-server start
redis-cli ping  # Verificar conexão

export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn test
```
**Tempo**: ~5-10 segundos  
**Requer**: Redis executando em localhost:6379  
**Quantidade de Testes**: 1 teste (teste de carregamento de contexto)  
**Saída Esperada**: Dados de café impressos no console durante o teste

### 4. Empacotar Aplicação
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn package
```
**Tempo**: ~11-15 segundos  
**Requer**: Redis executando (para testes)  
**Saída**: Cria `target/reactive-redis-coffe-0.0.1-SNAPSHOT.jar`

### 5. Executar Aplicação
```bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn spring-boot:run
```
**Porta**: 7878 (configurada em application.properties)  
**Requer**: Redis executando em localhost:6379  
**Endpoints**:
- GET `/sse` - Stream de Server-Sent Events com atualizações de café
- POST `/` - Adicionar novo café (aceita JSON: `{"name": "Nome do Café"}`)

## Estrutura do Projeto

### Arquivos Principais
```
.
├── pom.xml                          # Configuração de build Maven
├── .gitignore                       # Regras de ignore do Git
└── src/
    ├── main/
    │   ├── java/com/github/pedrobacchini/reactiverediscoffe/
    │   │   ├── ReactiveRedisCoffeApplication.java    # Ponto de entrada principal da aplicação
    │   │   ├── domain/
    │   │   │   └── Coffee.java                        # Modelo de domínio (usa Lombok @Data)
    │   │   ├── config/
    │   │   │   ├── CoffeeConfiguration.java           # Configuração de conexão Redis e serialização
    │   │   │   └── CoffeLoader.java                   # Carregador de dados @PostConstruct (popula Redis)
    │   │   └── resource/
    │   │       └── CoffeController.java               # Controlador REST com endpoint SSE
    │   └── resources/
    │       └── application.properties                 # Porta do servidor: 7878
    └── test/
        └── java/com/github/pedrobacchini/reactiverediscoffe/
            └── ReactiveRedisCoffeeApplicationTests.java  # Teste básico de carregamento de contexto
```

### Componentes da Arquitetura
1. **Domínio**: Entidade `Coffee` com id e nome (anotada com Lombok)
2. **Configuração**: Factory de conexão Redis (localhost:6379) e configuração de template reativo
3. **Carregador de Dados**: `CoffeLoader` popula Redis com 3 entradas de café na inicialização
4. **Controlador**: Endpoints REST com publishers reativos usando `ReplayProcessor` para SSE
5. **Integração Redis**: Operações reativas do Redis com serialização JSON via Jackson

### Dependências (do pom.xml)
- spring-boot-starter-data-redis-reactive
- spring-boot-starter-webflux
- spring-boot-devtools (runtime, opcional)
- lombok (opcional, processador de anotação)
- spring-boot-starter-test (escopo de teste)
- reactor-test (escopo de teste)

## Problemas Comuns e Soluções

### Build falha com IllegalAccessError
**Causa**: Não está usando Java 8  
**Solução**: Configure JAVA_HOME para Java 8 conforme mostrado acima

### Testes falham com "Unable to connect to Redis"
**Causa**: Redis não está executando  
**Solução**: Inicie o Redis com `sudo service redis-server start`

### Aplicação falha ao iniciar
**Causa**: Redis não disponível ou porta 7878 em uso  
**Solução**: Verifique o status do Redis e certifique-se de que a porta 7878 está livre

## Etapas de Validação

Após fazer alterações no código:
1. **Configure o ambiente Java 8** (sempre o primeiro passo)
2. **Certifique-se de que o Redis está executando** (para testes/execução)
3. **Limpe e compile**: `mvn clean compile`
4. **Execute os testes**: `mvn test` (se existirem testes para suas alterações)
5. **Empacote**: `mvn package` para criar o JAR executável
6. **Verificação manual**: Execute com `mvn spring-boot:run` e teste os endpoints com curl

## Observações Importantes

- **Sem GitHub Actions/CI**: Este repositório não possui workflows automatizados de CI/CD
- **Sem configuração de linting**: Não há checkstyle, PMD ou ferramentas similares configuradas
- **Cobertura mínima de testes**: Existe apenas um teste básico
- **Modo de desenvolvimento**: Usa spring-boot-devtools para hot reload
- **Configuração do Redis**: Hardcoded para localhost:6379 em `CoffeeConfiguration.java`
- **Porta do servidor**: Configurada para 7878 em `application.properties` (não é a porta padrão 8080)
- **Padrões reativos**: Usa Project Reactor com `ReplayProcessor` para streaming de eventos

## Confie Nestas Instruções

Estas instruções foram validadas executando cada comando no ambiente real. Busque informações adicionais apenas se estas instruções estiverem incompletas ou se você encontrar erros não documentados aqui.
