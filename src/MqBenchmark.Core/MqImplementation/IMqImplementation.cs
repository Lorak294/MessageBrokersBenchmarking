using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MqBenchmark.Core.MqImplementation;

/// <summary>
/// Interface representing a complete MQ implementation, encompassing consumer, producer, and janitor roles.
/// </summary>
public interface IMqImplementation
{
    IMqConsumer CreateConsumer();
    IMqProducer CreateProducer();
    IMqJanitor CreateJanitor();

    /// <summary>
    /// Method to retrieve key for registration in the DI container. Must be unique between all implementation.
    /// As Interface cannot have a static method without a body, throws NotImplementedException if not implemented,
    /// to force implementation in all concrete classes.
    /// </summary>
    /// <exception cref="NotImplementedException">if method not implemented</exception>
    static string GetKey() => throw new NotImplementedException("The GetKey method is not implemented for this implementation. Please add a static GetKey method to your implementation class that returns a unique string identifier for this MQ implementation.");
}