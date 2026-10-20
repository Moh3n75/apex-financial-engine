package com.apex.platform.messaging.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.EventContract;
import com.apex.platform.events.api.EventContracts;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.ContractEventHandler;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.OnCellEvent;

/** Real Boot imports discovery and JDBC transaction advice; no external resource is opened. */
@Tag("integration")
class AnnotatedConsumerIntegrationTest {
    private final List<Connection> connections = new ArrayList<>();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MinimalConsumer.class)
            .withInitializer(context -> {
                context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
                context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            })
            .withPropertyValues("apex.messaging.cell.enabled=true", "apex.messaging.consumer.enabled=true",
                    "apex.messaging.outbox.publisher.enabled=false", "spring.application.name=m25-consumer",
                    "apex.cell.id=m25-cell", "spring.kafka.consumer.group-id=m25-tests",
                    "apex.messaging.consumer.topics=m25.test.events", "spring.kafka.bootstrap-servers=127.0.0.1:1",
                    "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false")
            .withBean(DataSource.class, this::dataSource);

    @Test
    void discoversSpringComponentsThroughBootAutoConfiguration() {
        runner.withUserConfiguration(ScannedConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1)).isPresent();
            assertThat(context.getBeansOfType(ScannedHandler.class)).hasSize(1);
            assertThat(context.getBeansOfType(EventHandler.class)).isEmpty();
        });
    }

    @Test
    void discoversMultipleMethodsAndMultipleBeans() {
        runner.withBean("multiple", MultipleHandler.class).withBean("single", SingleHandler.class).run(context -> {
            assertThat(context).hasNotFailed();
            EventHandlerRegistry registry = context.getBean(EventHandlerRegistry.class);
            assertThat(registry.find("M25_CREATED", 1)).isPresent();
            assertThat(registry.find("M25_FIRST", 1)).isPresent();
            assertThat(registry.find("M25_SECOND", 2)).isPresent();
        });
    }

    @Test
    void resolvesAnnotationsOnAnObjectReturningBeanFactory() {
        runner.withUserConfiguration(ObjectFactory.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1)).isPresent();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void handlerMayDependOnTheRegistryWithoutACreationCycle(boolean reverseOrder) {
        var ordered = reverseOrder
                ? runner.withBean("multiple", MultipleHandler.class).withBean("aware", RegistryAwareHandler.class)
                : runner.withBean("aware", RegistryAwareHandler.class).withBean("multiple", MultipleHandler.class);
        ordered.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1)).isPresent();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_FIRST", 1)).isPresent();
        });
    }

    @Test
    void discoversALazyAnnotatedSingletonBeforeListenersStart() {
        runner.withBean("lazyHandler", SingleHandler.class, SingleHandler::new, definition -> definition.setLazyInit(true)).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1)).isPresent();
        });
    }

    @Test
    void discoversTheFactoryBeanProductRatherThanItsFactory() {
        runner.withBean("factoryHandler", HandlerFactory.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1)).isPresent();
        });
    }

    @Test
    void rejectsPrototypeHandlersInsteadOfCapturingAnAccidentalSingletonInstance() {
        runner.withBean("prototypeHandler", SingleHandler.class, SingleHandler::new, definition -> definition.setScope("prototype"))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("prototypeHandler")
                            .hasStackTraceContaining("singleton");
                });
    }

    @Test
    void consumerDisabledDoesNotRunDiscoveryOrRejectAnUnusedAnnotatedMethod() {
        runner.withPropertyValues("apex.messaging.consumer.enabled=false")
                .withBean("unusedInvalidHandler", PrivateMethod.class).run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(EventHandlerRegistry.class);
                });
    }

    @Test
    void retainsLegacyContractHandlersAlongsideAnnotatedHandlers() {
        LegacyHandler legacy = new LegacyHandler("M25_LEGACY");
        runner.withBean("legacy", EventHandler.class, () -> legacy).withBean("single", SingleHandler.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    EventHandlerRegistry registry = context.getBean(EventHandlerRegistry.class);
                    assertThat(registry.find("M25_LEGACY", 1).orElseThrow()).isSameAs(legacy);
                    assertThat(registry.find("M25_CREATED", 1)).isPresent();
                    assertThat(context.getBeansOfType(EventHandler.class)).hasSize(1);
                });
    }

    @Test
    void rejectsDuplicatesAcrossAnnotatedBeans() {
        runner.withBean("first", SingleHandler.class).withBean("second", SingleHandler.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("Duplicate event handler")
                    .hasStackTraceContaining("M25_CREATED").hasStackTraceContaining("first").hasStackTraceContaining("second");
        });
    }

    @Test
    void rejectsDuplicatesAcrossLegacyAndAnnotatedHandlers() {
        runner.withBean("legacy", EventHandler.class, () -> new LegacyHandler("M25_CREATED"))
                .withBean("single", SingleHandler.class).run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("Duplicate event handler")
                            .hasStackTraceContaining("M25_CREATED").hasStackTraceContaining("single");
                });
    }

    @Test
    void retainsTheOriginalEventHandlerInterfaceWithoutRequiringAContract() {
        DirectLegacyHandler legacy = new DirectLegacyHandler();
        runner.withBean("directLegacy", EventHandler.class, () -> legacy).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(EventHandlerRegistry.class).find("M25_DIRECT", 1).orElseThrow()).isSameAs(legacy);
        });
    }

    @Test
    void rejectsDuplicateMethodsOnOneBean() {
        runner.withBean("duplicateMethods", DuplicateMethods.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("Duplicate event handler")
                    .hasStackTraceContaining("duplicateMethods").hasStackTraceContaining("first").hasStackTraceContaining("second");
        });
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("invalidMethods")
    void rejectsInvalidSignaturesAtStartup(Class<?> handlerClass, String reason) {
        runner.withBean("badHandler", handlerClass).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("badHandler")
                    .hasStackTraceContaining("handle").hasStackTraceContaining(reason);
        });
    }

    static Stream<Arguments> invalidMethods() {
        return Stream.of(
                Arguments.of(NoPayload.class, "payload"), Arguments.of(TooMany.class, "payload"),
                Arguments.of(WrongMetadata.class, "EventMetadata"), Arguments.of(NonVoid.class, "void"),
                Arguments.of(BlankType.class, "event type"), Arguments.of(ZeroVersion.class, "version"),
                Arguments.of(NegativeVersion.class, "version"), Arguments.of(PrivateMethod.class, "public"),
                Arguments.of(StaticMethod.class, "instance"), Arguments.of(PrimitivePayload.class, "payload"),
                Arguments.of(ObjectPayload.class, "payload"), Arguments.of(InterfacePayload.class, "payload"),
                Arguments.of(AbstractPayload.class, "payload"), Arguments.of(GenericPayload.class, "payload"),
                Arguments.of(RawListPayload.class, "payload"), Arguments.of(TypeVariablePayload.class, "payload"));
    }

    @ParameterizedTest(name = "invokes Spring proxy, CGLIB={0}")
    @ValueSource(booleans = {true, false})
    void invokesTheSpringProxyRatherThanTheRawTarget(boolean cglib) {
        ProxyHandler target = new ProxyHandler();
        AtomicInteger adviceCalls = new AtomicInteger();
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(cglib);
        factory.addAdvice((MethodInterceptor) invocation -> {
            adviceCalls.incrementAndGet();
            return invocation.proceed();
        });
        Object proxy = factory.getProxy();
        runner.withBean("proxiedHandler", Object.class, () -> proxy).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(AopUtils.isAopProxy(context.getBean("proxiedHandler"))).isTrue();
            EventHandler<?> raw = context.getBean(EventHandlerRegistry.class).find("M25_PROXY", 1).orElseThrow();
            invoke(raw, "M25_PROXY");
            assertThat(target.payload).isEqualTo(new Payload("value"));
            assertThat(adviceCalls).hasValue(1);
        });
    }

    @Test
    void rejectsAJdkProxyThatDoesNotExposeTheHandlerMethod() {
        ProxyFactory factory = new ProxyFactory(new IncompatibleProxyHandler());
        factory.setProxyTargetClass(false);
        Object proxy = factory.getProxy();
        runner.withBean("badProxy", Object.class, () -> proxy).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("badProxy")
                    .hasStackTraceContaining("handle").hasStackTraceContaining("not invocable");
        });
    }

    @Test
    void rejectsAFinalHandlerMethodOnACglibProxy() {
        ProxyFactory factory = new ProxyFactory(new FinalMethodHandler());
        factory.setProxyTargetClass(true);
        Object proxy = factory.getProxy();
        runner.withBean("finalProxy", Object.class, () -> proxy).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("finalProxy")
                    .hasStackTraceContaining("handle").hasStackTraceContaining("final");
        });
    }

    @Test
    void publicMethodOnAPackagePrivateSpringBeanRemainsInvocable() {
        PackagePrivateHandler target = new PackagePrivateHandler();
        runner.withBean("packagePrivate", PackagePrivateHandler.class, () -> target).run(context -> {
            assertThat(context).hasNotFailed();
            invoke(context.getBean(EventHandlerRegistry.class).find("M25_CREATED", 1).orElseThrow(), "M25_CREATED");
            assertThat(target.called).isTrue();
        });
    }

    @Test
    void preservesSpringCreatedTransactionAdviceOnTheHandler() {
        TransactionalHandler target = new TransactionalHandler();
        runner.withBean("transactionalHandler", TransactionalHandler.class, () -> target).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(AopUtils.isAopProxy(context.getBean("transactionalHandler"))).isTrue();
            invoke(context.getBean(EventHandlerRegistry.class).find("M25_TX", 1).orElseThrow(), "M25_TX");
            assertThat(target.activeTransaction).isTrue();
            assertThat(connections).hasSize(1);
            verify(connections.getFirst()).commit();
        });
    }

    private DataSource dataSource() {
        DataSource dataSource = mock(DataSource.class);
        try {
            when(dataSource.getConnection()).thenAnswer(invocation -> {
                Connection connection = mock(Connection.class);
                when(connection.getAutoCommit()).thenReturn(true);
                connections.add(connection);
                return connection;
            });
        } catch (SQLException exception) {
            throw new IllegalStateException(exception);
        }
        return dataSource;
    }

    @SuppressWarnings("unchecked")
    private static void invoke(EventHandler<?> handler, String type) {
        ((EventHandler<Payload>) handler).handle(envelope(type), delivery());
    }

    public static EventEnvelope<Payload> envelope(String type) {
        return new EventEnvelope<>(new EventMetadata(UUID.randomUUID(), type, 1, UUID.randomUUID(), null,
                "test-1", "TEST", "producer", "producer-cell", Instant.EPOCH), new Payload("value"));
    }

    public static ConsumerContext delivery() {
        return new ConsumerContext("m25-tests", "m25.test.events", 0, 1, "test-1", Instant.EPOCH);
    }

    public record Payload(String value) {}

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class MinimalConsumer {}

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackageClasses = AnnotatedConsumerIntegrationTest.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ScannedHandler.class))
    static class ScannedConfiguration {}

    @Component
    public static class ScannedHandler extends SingleHandler {}

    public static class SingleHandler {
        @OnCellEvent(type = "M25_CREATED", version = 1)
        public void handle(Payload payload) {}
    }

    public static class MultipleHandler {
        @OnCellEvent(type = "M25_FIRST", version = 1)
        public void first(Payload payload) {}
        @OnCellEvent(type = "M25_SECOND", version = 2)
        public void second(Payload payload, EventMetadata metadata) {}
    }

    @Configuration(proxyBeanMethods = false)
    static class ObjectFactory {
        @Bean Object obscureHandler() { return new SingleHandler(); }
    }

    public static class RegistryAwareHandler extends SingleHandler {
        public RegistryAwareHandler(EventHandlerRegistry registry) {}
    }

    public static class HandlerFactory implements FactoryBean<Object> {
        public Object getObject() { return new SingleHandler(); }
        public Class<?> getObjectType() { return Object.class; }
        public boolean isSingleton() { return true; }
    }

    public static class DuplicateMethods {
        @OnCellEvent(type = "DUPLICATE", version = 1) public void first(Payload payload) {}
        @OnCellEvent(type = "DUPLICATE", version = 1) public void second(Payload payload) {}
    }

    public static class DirectLegacyHandler implements EventHandler<Payload> {
        public String eventType() { return "M25_DIRECT"; }
        public int eventVersion() { return 1; }
        public Class<Payload> payloadType() { return Payload.class; }
        public void handle(EventEnvelope<Payload> event, ConsumerContext context) {}
    }

    public static class LegacyHandler implements ContractEventHandler<Payload> {
        private final String type;
        LegacyHandler(String type) { this.type = type; }
        public EventContract<Payload> contract() { return EventContracts.define(type, 1, "TEST", Payload.class, "m25.test.v1"); }
        public void handle(EventEnvelope<Payload> event, ConsumerContext context) {}
    }

    public interface ProxyApi { void handle(Payload payload, EventMetadata metadata); }
    public static class ProxyHandler implements ProxyApi {
        Payload payload;
        @OnCellEvent(type = "M25_PROXY", version = 1)
        public void handle(Payload payload, EventMetadata metadata) { this.payload = payload; }
    }
    public interface OtherApi { void other(); }
    public static class IncompatibleProxyHandler extends SingleHandler implements OtherApi {
        public void other() {}
    }
    public static class FinalMethodHandler {
        @OnCellEvent(type = "M25_FINAL", version = 1)
        public final void handle(Payload payload) {}
    }
    static class PackagePrivateHandler {
        boolean called;
        @OnCellEvent(type = "M25_CREATED", version = 1)
        public void handle(Payload payload) { called = true; }
    }
    public static class TransactionalHandler {
        boolean activeTransaction;
        @Transactional
        @OnCellEvent(type = "M25_TX", version = 1)
        public void handle(Payload payload) { activeTransaction = TransactionSynchronizationManager.isActualTransactionActive(); }
    }
    public static class NoPayload { @OnCellEvent(type = "X", version = 1) public void handle() {} }
    public static class TooMany { @OnCellEvent(type = "X", version = 1) public void handle(Payload p, EventMetadata m, String extra) {} }
    public static class WrongMetadata { @OnCellEvent(type = "X", version = 1) public void handle(Payload p, String extra) {} }
    public static class NonVoid { @OnCellEvent(type = "X", version = 1) public String handle(Payload p) { return ""; } }
    public static class BlankType { @OnCellEvent(type = " ", version = 1) public void handle(Payload p) {} }
    public static class ZeroVersion { @OnCellEvent(type = "X", version = 0) public void handle(Payload p) {} }
    public static class NegativeVersion { @OnCellEvent(type = "X", version = -1) public void handle(Payload p) {} }
    public static class PrivateMethod { @OnCellEvent(type = "X", version = 1) private void handle(Payload p) {} }
    public static class StaticMethod { @OnCellEvent(type = "X", version = 1) public static void handle(Payload p) {} }
    public static class PrimitivePayload { @OnCellEvent(type = "X", version = 1) public void handle(int p) {} }
    public static class ObjectPayload { @OnCellEvent(type = "X", version = 1) public void handle(Object p) {} }
    public static class InterfacePayload { @OnCellEvent(type = "X", version = 1) public void handle(Runnable p) {} }
    public static class AbstractPayload { @OnCellEvent(type = "X", version = 1) public void handle(Number p) {} }
    public static class GenericPayload { @OnCellEvent(type = "X", version = 1) public void handle(List<Payload> p) {} }
    @SuppressWarnings("rawtypes")
    public static class RawListPayload { @OnCellEvent(type = "X", version = 1) public void handle(List p) {} }
    public static class TypeVariablePayload { @OnCellEvent(type = "X", version = 1) public <T> void handle(T p) {} }
}
