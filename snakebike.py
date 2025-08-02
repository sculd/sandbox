import pygame
import random
import sys
import math

# Initialize pygame
pygame.init()

# Constants
WIDTH, HEIGHT = 800, 600
GRID_SIZE = 20
GRID_WIDTH = WIDTH // GRID_SIZE
GRID_HEIGHT = HEIGHT // GRID_SIZE
FPS = 10

# Colors
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
GREEN = (0, 255, 0)
RED = (255, 0, 0)
BLUE = (0, 0, 255)

# Direction vectors
UP = (0, -1)
DOWN = (0, 1)
LEFT = (-1, 0)
RIGHT = (1, 0)

class SnakeBike:
    def __init__(self):
        self.screen = pygame.display.set_mode((WIDTH, HEIGHT))
        pygame.display.set_caption("SnakeBike Game")
        self.clock = pygame.time.Clock()
        self.font = pygame.font.SysFont(None, 36)
        self.reset_game()
        
    def reset_game(self):
        # Starting position at center
        self.snake = [(GRID_WIDTH // 2, GRID_HEIGHT // 2)]
        self.direction = RIGHT
        self.next_direction = RIGHT
        self.food = self.place_food()
        self.score = 0
        self.game_over = False
        self.angle = 0  # Angle in degrees for bike-like movement
        
    def place_food(self):
        while True:
            food = (random.randint(0, GRID_WIDTH - 1), random.randint(0, GRID_HEIGHT - 1))
            if food not in self.snake:
                return food
                
    def handle_keys(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_UP and self.direction != DOWN:
                    self.next_direction = UP
                    self.angle = 270
                elif event.key == pygame.K_DOWN and self.direction != UP:
                    self.next_direction = DOWN
                    self.angle = 90
                elif event.key == pygame.K_LEFT and self.direction != RIGHT:
                    self.next_direction = LEFT
                    self.angle = 180
                elif event.key == pygame.K_RIGHT and self.direction != LEFT:
                    self.next_direction = RIGHT
                    self.angle = 0
                elif event.key == pygame.K_r and self.game_over:
                    self.reset_game()
                    
    def move_snake(self):
        if self.game_over:
            return
            
        self.direction = self.next_direction
        
        # Calculate new head position
        head_x, head_y = self.snake[0]
        dx, dy = self.direction
        new_head = ((head_x + dx) % GRID_WIDTH, (head_y + dy) % GRID_HEIGHT)
        
        # Check collision with self
        if new_head in self.snake:
            self.game_over = True
            return
            
        # Move snake
        self.snake.insert(0, new_head)
        
        # Check if food eaten
        if new_head == self.food:
            self.score += 1
            self.food = self.place_food()
        else:
            self.snake.pop()
            
    def draw_bike_head(self, position, angle):
        x, y = position
        rect = pygame.Rect(x * GRID_SIZE, y * GRID_SIZE, GRID_SIZE, GRID_SIZE)
        
        # Draw basic head square
        pygame.draw.rect(self.screen, GREEN, rect)
        
        # Calculate front point (as a bike would have a front)
        center_x = x * GRID_SIZE + GRID_SIZE // 2
        center_y = y * GRID_SIZE + GRID_SIZE // 2
        front_length = GRID_SIZE // 2
        front_x = center_x + math.cos(math.radians(angle)) * front_length
        front_y = center_y + math.sin(math.radians(angle)) * front_length
        
        # Draw front pointer (like a handlebar)
        pygame.draw.line(self.screen, BLUE, (center_x, center_y), (front_x, front_y), 3)
        
        # Draw small circle as a headlight
        headlight_radius = GRID_SIZE // 6
        pygame.draw.circle(self.screen, WHITE, (int(front_x), int(front_y)), headlight_radius)
            
    def draw(self):
        self.screen.fill(BLACK)
        
        # Draw snake
        for i, segment in enumerate(self.snake):
            x, y = segment
            rect = pygame.Rect(x * GRID_SIZE, y * GRID_SIZE, GRID_SIZE, GRID_SIZE)
            
            if i == 0:  # Head
                self.draw_bike_head(segment, self.angle)
            else:  # Body
                # Make the body gradually fade to a darker green
                fade_factor = max(0.3, 1 - (i / len(self.snake)))
                segment_color = (0, int(255 * fade_factor), 0)
                pygame.draw.rect(self.screen, segment_color, rect)
                
                # Draw bike-like connecting lines
                if i < len(self.snake) - 1:
                    current_center = (x * GRID_SIZE + GRID_SIZE // 2, y * GRID_SIZE + GRID_SIZE // 2)
                    next_x, next_y = self.snake[i + 1]
                    next_center = (next_x * GRID_SIZE + GRID_SIZE // 2, next_y * GRID_SIZE + GRID_SIZE // 2)
                    pygame.draw.line(self.screen, WHITE, current_center, next_center, 2)
        
        # Draw food
        food_x, food_y = self.food
        food_rect = pygame.Rect(food_x * GRID_SIZE, food_y * GRID_SIZE, GRID_SIZE, GRID_SIZE)
        pygame.draw.rect(self.screen, RED, food_rect)
        
        # Draw score
        score_text = self.font.render(f"Score: {self.score}", True, WHITE)
        self.screen.blit(score_text, (10, 10))
        
        # Draw game over message
        if self.game_over:
            game_over_text = self.font.render("Game Over! Press R to restart", True, WHITE)
            text_rect = game_over_text.get_rect(center=(WIDTH // 2, HEIGHT // 2))
            self.screen.blit(game_over_text, text_rect)
            
        pygame.display.flip()
        
    def run(self):
        while True:
            self.handle_keys()
            self.move_snake()
            self.draw()
            self.clock.tick(FPS)

# Run the game
if __name__ == "__main__":
    game = SnakeBike()
    game.run()
