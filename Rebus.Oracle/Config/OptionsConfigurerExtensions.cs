using Rebus.Retry;

namespace Rebus.Config
{
    public static class OptionsConfigurerExtensions
    {
        public static OptionsConfigurer UseOracleAQRetryStrategy(this OptionsConfigurer optionsConfigurer)
        {
            optionsConfigurer.Register<IRetryStrategy>(c =>
            {
                return new OracleAQRetryStrategy();
            });
            return optionsConfigurer;
        }
    }
}
