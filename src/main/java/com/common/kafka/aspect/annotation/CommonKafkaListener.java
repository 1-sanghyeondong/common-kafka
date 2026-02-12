package com.common.kafka.aspect.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

@KafkaListener
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface CommonKafkaListener {

    /**
     * 리스너 컨테이너의 고유 식별자 (ID)
     * JMX, 로그, 메트릭 등에서 리스너를 구분할 때 사용
     * 지정하지 않으면 스프링이 자동 생성한 ID 가 자동 부여
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "id")
    String id() default "";

    /**
     * 구독할 토픽 목록
     * ex/ topics = {"order-events", "payment-events"}
     * SpEL 표현식 사용도 가능 (ex/ "${kafka.topic.name}")
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String[] topics() default {};

    /**
     * 컨슈머 그룹 ID
     * application.yml 에 설정된 기본 그룹 ID 보다 우선 적용
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "groupId")
    String groupId() default "";

    /**
     * 사용할 리스너 컨테이너 팩토리 빈 이름
     *
     * 기본값: "commonContainerFactory" (라이브러리 전용 팩토리)
     * 기존 프로젝트의 "kafkaListenerContainerFactory" 와 충돌하지 않음
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "containerFactory")
    String containerFactory() default "commonContainerFactory";

    /**
     * 병렬 처리 스레드 수 (concurrency)
     * 하나의 리스너가 몇 개의 스레드로 동시에 메시지를 처리할지 설정
     * ex/ "3"으로 설정하면 3개의 컨슈머 스레드가 동시에 동작
     * (주의: 파티션 개수보다 많게 설정하면 잉여 스레드가 발생할 수 있음)
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "concurrency")
    String concurrency() default "";

    /**
     * Resiliency 기능 활성화 여부
     * <p>
     * true (기본값): 실패 시 retry 토픽으로 전송 및 blocking 처리
     * false: 실패 시 예외를 즉시 throw (retry/dead letter 처리 안 함)
     */
    boolean enableResiliency() default true;
}