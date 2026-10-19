package com.niranjankagri.orders.order;

import graphql.GraphQLError;
import graphql.schema.DataFetchingEnvironment;
import jakarta.validation.ConstraintViolationException;
import java.util.stream.Collectors;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.stereotype.Component;

/**
 * Turns invalid input into a GraphQL error the client can act on (classification BAD_REQUEST, the broken rules
 * in the message) instead of the default INTERNAL_ERROR without details.
 */
@Component
public class GraphQlErrors extends DataFetcherExceptionResolverAdapter {

    /**
     * Bean-validation failures become one BAD_REQUEST error listing every broken field (sorted, so the message is
     * stable); any other exception is left to Spring's default.
     */
    @Override
    protected GraphQLError resolveToSingleError(Throwable ex, DataFetchingEnvironment env) {
        if (ex instanceof ConstraintViolationException violations) {
            String message = violations.getConstraintViolations().stream()
                    .map(v -> lastSegment(v.getPropertyPath().toString()) + ": " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            return GraphQLError.newError()
                    .errorType(ErrorType.BAD_REQUEST)
                    .message(message)
                    .path(env.getExecutionStepInfo().getPath())
                    .location(env.getField().getSourceLocation())
                    .build();
        }
        return null; // everything else: Spring's default handling (no internal details leak)
    }

    /** "placeOrder.input.quantity" becomes "quantity". */
    private static String lastSegment(String path) {
        return path.substring(path.lastIndexOf('.') + 1);
    }
}
