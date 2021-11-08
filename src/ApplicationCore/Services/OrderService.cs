using Ardalis.GuardClauses;
using Microsoft.Azure.ServiceBus;
using Microsoft.eShopWeb.ApplicationCore.Entities;
using Microsoft.eShopWeb.ApplicationCore.Entities.BasketAggregate;
using Microsoft.eShopWeb.ApplicationCore.Entities.OrderAggregate;
using Microsoft.eShopWeb.ApplicationCore.Interfaces;
using Microsoft.eShopWeb.ApplicationCore.Specifications;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.eShopWeb.ApplicationCore.Services
{
    public class OrderService : IOrderService
    {
        private readonly IAsyncRepository<Order> _orderRepository;
        private readonly IUriComposer _uriComposer;
        private readonly IAsyncRepository<Basket> _basketRepository;
        private readonly IAsyncRepository<CatalogItem> _itemRepository;
        private readonly IConfiguration _configuration;

        public OrderService(IAsyncRepository<Basket> basketRepository,
            IAsyncRepository<CatalogItem> itemRepository,
            IAsyncRepository<Order> orderRepository,
            IUriComposer uriComposer,
            IConfiguration configuration)
        {
            _orderRepository = orderRepository;
            _uriComposer = uriComposer;
            _basketRepository = basketRepository;
            _itemRepository = itemRepository;
            _configuration = configuration;
        }

        public async Task CreateOrderAsync(int basketId, Address shippingAddress)
        {
            var basketSpec = new BasketWithItemsSpecification(basketId);
            var basket = await _basketRepository.FirstOrDefaultAsync(basketSpec);

            Guard.Against.NullBasket(basketId, basket);
            Guard.Against.EmptyBasketOnCheckout(basket.Items);

            var catalogItemsSpecification = new CatalogItemsSpecification(basket.Items.Select(item => item.CatalogItemId).ToArray());
            var catalogItems = await _itemRepository.ListAsync(catalogItemsSpecification);

            var items = basket.Items.Select(basketItem =>
            {
                var catalogItem = catalogItems.First(c => c.Id == basketItem.CatalogItemId);
                var itemOrdered = new CatalogItemOrdered(catalogItem.Id, catalogItem.Name, _uriComposer.ComposePicUri(catalogItem.PictureUri));
                var orderItem = new OrderItem(itemOrdered, basketItem.UnitPrice, basketItem.Quantity);
                return orderItem;
            }).ToList();

            var order = new Order(basket.BuyerId, shippingAddress, items);

            await _orderRepository.AddAsync(order);

            // Calling OrderItemsReserver funtion with http trigger.
            var serializedItemsForBlob = JsonConvert.SerializeObject(order);
            var httpClient = new HttpClient();
            var url = _configuration.GetSection("DeliveryOrderProcessorUrl").Value;

            await httpClient.PostAsync(url, new StringContent(serializedItemsForBlob));

            // Calling sending message to Service Bus, so it would trigger DeliveryOrderProcessor function.
            var itemsForServiceBus = items.Select(item => new { Id = item.Id, Quantity = item.Units });
            var serializedItemsForServiceBus = JsonConvert.SerializeObject(itemsForServiceBus);
            var serviceBusConnectionString = _configuration.GetConnectionString("ServiceBus");
            var serviceBusQueueName = _configuration.GetSection("ServiceBusQueueName").Value;
            var queueClient = new QueueClient(serviceBusConnectionString, serviceBusQueueName);
            var message = new Message(Encoding.UTF8.GetBytes(serializedItemsForServiceBus));

            await queueClient.SendAsync(message);
            await queueClient.CloseAsync();
        }
    }
}
